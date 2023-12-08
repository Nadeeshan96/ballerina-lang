/*
 * Copyright (c) 2023, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.runtime.internal.util;

import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.flags.SymbolFlags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.utils.ValueUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.internal.TypeChecker;
import io.ballerina.runtime.internal.TypeConverter;
import io.ballerina.runtime.internal.types.BMapType;
import io.ballerina.runtime.internal.types.BRecordType;
import io.ballerina.runtime.internal.types.BUnionType;
import io.ballerina.runtime.internal.values.ArrayValueImpl;
import io.ballerina.runtime.internal.values.DecimalValue;
import io.ballerina.runtime.internal.values.MapValueImpl;
import io.ballerina.runtime.internal.values.TupleValueImpl;
import org.apache.commons.lang3.StringEscapeUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.ballerina.runtime.internal.ValueUtils.createRecordValueWithDefaultValues;

/**
 * This class represents an input stream parser.
 *
 * @since 2201.9.0
 */
@SuppressWarnings("unchecked")
public class StreamParser {

    private static final ThreadLocal<StateMachine> tlStateMachine = ThreadLocal.withInitial(StateMachine::new);

    private StreamParser() {
    }

    /**
     * Parses the contents in the given {@link InputStream} and returns a value.
     *
     * @param in input stream which contains the JSON content
     * @return JSON structure
     * @throws BError for any parsing error
     */
    public static Object parse(InputStream in, Type targetType) throws BError {
        return parse(in, Charset.defaultCharset().name(), targetType);
    }

    /**
     * Parses the contents in the given {@link InputStream} and returns a json.
     *
     * @param in          input stream which contains the JSON content
     * @param charsetName the character set name of the input stream
     * @return JSON structure
     * @throws BError for any parsing error
     */
    private static Object parse(InputStream in, String charsetName, Type targetType) throws BError {
        try {
            return parse(new InputStreamReader(new BufferedInputStream(in), charsetName), targetType);
        } catch (IOException e) {
            throw ErrorCreator
                    .createError(StringUtils.fromString(("Error in parsing data: " + e.getMessage())));
        }
    }

    /**
     * Parses the contents in the given string and returns a json.
     *
     * @param jsonStr the string which contains the JSON content
     * @return JSON structure
     * @throws BError for any parsing error
     */
    public static Object parse(String jsonStr, Type targetType) throws BError {
        return parse(new StringReader(jsonStr), targetType);
    }

    /**
     * Parses the contents in the given {@link Reader} and returns a json.
     *
     * @param reader reader which contains the JSON content
     * @return JSON structure
     * @throws BError for any parsing error
     */
    private static Object parse(Reader reader, Type targetType) throws BError {
        StateMachine sm = tlStateMachine.get();
        try {
            sm.addTargetType(targetType);
            return sm.execute(reader);
        } finally {
            // Need to reset the state machine before leaving. Otherwise, references to the created
            // JSON values will be maintained and the java GC will not happen properly.
            sm.reset();
        }
    }

    /**
     * Represents a JSON parser related exception.
     */
    private static class StreamParserException extends Exception {

        public StreamParserException(String msg) {
            super(msg);
        }

    }

    private static Object convertValues(Type targetType, String inputValue) throws StreamParserException {
        switch (targetType.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.SIGNED32_INT_TAG:
            case TypeTags.SIGNED16_INT_TAG:
            case TypeTags.SIGNED8_INT_TAG:
            case TypeTags.UNSIGNED32_INT_TAG:
            case TypeTags.UNSIGNED16_INT_TAG:
            case TypeTags.UNSIGNED8_INT_TAG:
                try {
                    return Long.parseLong(inputValue);
                } catch (NumberFormatException e) {
                    throw new StreamParserException("error");
                }
            case TypeTags.DECIMAL_TAG:
                try {
                    return new DecimalValue(inputValue);
                } catch (NumberFormatException e) {
                    throw new StreamParserException("error");
                }
            case TypeTags.FLOAT_TAG:
                try {
                    return Double.parseDouble(inputValue);
                } catch (NumberFormatException e) {
                    throw new StreamParserException("error");
                }
            case TypeTags.STRING_TAG:
                throw new StreamParserException("not a string");
            case TypeTags.BOOLEAN_TAG:
                char ch = inputValue.charAt(0);
                if (ch == 't' && StateMachine.TRUE.equals(inputValue)) {
                    return Boolean.TRUE;
                } else if (ch == 'f' && StateMachine.FALSE.equals(inputValue)) {
                    return Boolean.FALSE;
                } else {
                    throw new StreamParserException("error");
                }
            case TypeTags.NULL_TAG:
                if (inputValue.charAt(0) == 'n' && StateMachine.NULL.equals(inputValue)) {
                    return null;
                } else {
                    throw new StreamParserException("error");
                }
            case TypeTags.BYTE_TAG:
                try {
                    return Integer.parseInt(inputValue);
                } catch (NumberFormatException e) {
                    throw new StreamParserException("error");
                }
            default:
                throw new StreamParserException("error");
        }
    }

    /**
     * Represents the state machine used for JSON parsing.
     */
    private static class StateMachine {

        private static final char CR = 0x000D;
        private static final char NEWLINE = 0x000A;
        private static final char HZ_TAB = 0x0009;
        private static final char SPACE = 0x0020;
        private static final char BACKSPACE = 0x0008;
        private static final char FORMFEED = 0x000C;
        private static final char QUOTES = '"';
        private static final char REV_SOL = '\\';
        private static final char SOL = '/';
        private static final char EOF = (char) -1;
        private static final String NULL = "null";
        private static final String TRUE = "true";
        private static final String FALSE = "false";

        private static final State DOC_START_STATE = new DocumentStartState();
        private static final State DOC_END_STATE = new DocumentEndState();
        private static final State FIRST_FIELD_READY_STATE = new FirstFieldReadyState();
        private static final State NON_FIRST_FIELD_READY_STATE = new NonFirstFieldReadyState();
        private static final State FIELD_NAME_STATE = new FieldNameState();
        private static final State END_FIELD_NAME_STATE = new EndFieldNameState();
        private static final State FIELD_VALUE_READY_STATE = new FieldValueReadyState();
        private static final State STRING_FIELD_VALUE_STATE = new StringFieldValueState();
        private static final State NON_STRING_FIELD_VALUE_STATE = new NonStringFieldValueState();
        private static final State NON_STRING_VALUE_STATE = new NonStringValueState();
        private static final State STRING_VALUE_STATE = new StringValueState();
        private static final State FIELD_END_STATE = new FieldEndState();
        private static final State STRING_AE_ESC_CHAR_PROCESSING_STATE = new StringAEEscapedCharacterProcessingState();
        private static final State STRING_AE_PROCESSING_STATE = new StringAEProcessingState();
        private static final State FIELD_NAME_UNICODE_HEX_PROCESSING_STATE = new FieldNameUnicodeHexProcessingState();
        private static final State FIRST_ARRAY_ELEMENT_READY_STATE = new FirstArrayElementReadyState();
        private static final State NON_FIRST_ARRAY_ELEMENT_READY_STATE = new NonFirstArrayElementReadyState();
        private static final State STRING_ARRAY_ELEMENT_STATE = new StringArrayElementState();
        private static final State NON_STRING_ARRAY_ELEMENT_STATE = new NonStringArrayElementState();
        private static final State ARRAY_ELEMENT_END_STATE = new ArrayElementEndState();
        private static final State STRING_FIELD_ESC_CHAR_PROCESSING_STATE =
                new StringFieldEscapedCharacterProcessingState();
        private static final State STRING_VAL_ESC_CHAR_PROCESSING_STATE =
                new StringValueEscapedCharacterProcessingState();
        private static final State FIELD_NAME_ESC_CHAR_PROCESSING_STATE =
                new FieldNameEscapedCharacterProcessingState();
        private static final State STRING_FIELD_UNICODE_HEX_PROCESSING_STATE =
                new StringFieldUnicodeHexProcessingState();
        private static final State STRING_VALUE_UNICODE_HEX_PROCESSING_STATE =
                new StringValueUnicodeHexProcessingState();
        private Type definedJsonType = PredefinedTypes.TYPE_JSON;

        List<Type> targetTypes = new ArrayList<>(); // if the target type is union we put the union here,
        // but we create a json value and convert to the target type
        // we can optimize this tuple|array - but will do later

        List<Integer> listIndices = new ArrayList<>(); // we keep only the current indices of arrays and tuples,
        // so this one's size may be less than the former
        // doing like this rather than creating a wrapper to decrease memory usage
        // - using this uses same memory as function call stack

        List<List<Type>> possibleTypes = new ArrayList<>();

        private Object currentJsonNode;
        private Deque<Object> nodesStack;
        private Deque<String> fieldNames;

        private StringBuilder hexBuilder = new StringBuilder(4);
        private char[] charBuff = new char[1024];
        private int charBuffIndex;

        private int index;
        private int line;
        private int column;
        private char currentQuoteChar;

        StateMachine() {
            reset();
        }

        public void reset() {
            this.index = 0;
            this.currentJsonNode = null;
            this.line = 1;
            this.column = 0;
            this.nodesStack = new ArrayDeque<>();
            this.fieldNames = new ArrayDeque<>();
            this.targetTypes = new ArrayList<>();
        }

        private void addTargetType(Type type) {
            this.targetTypes.add(TypeUtils.getImpliedType(type));
        }

        private static boolean isWhitespace(char ch) {
            return ch == SPACE || ch == HZ_TAB || ch == NEWLINE || ch == CR;
        }

        private static void throwExpected(String... chars) throws StreamParserException {
            throw new StreamParserException("expected '" + String.join("' or '", chars) + "'");
        }

        private void processLocation(char ch) {
            if (ch == '\n') {
                this.line++;
                this.column = 0;
            } else {
                this.column++;
            }
        }

        public Object execute(Reader reader) throws BError {
            State currentState = DOC_START_STATE;
            try {
                char[] buff = new char[1024];
                int count;
                while ((count = reader.read(buff)) > 0) {
                    this.index = 0;
                    while (this.index < count) {
                        currentState = currentState.transition(this, buff, this.index, count);
                    }
                }
                currentState = currentState.transition(this, new char[] { EOF }, 0, 1);
                if (currentState != DOC_END_STATE) {
                    throw ErrorCreator.createError(StringUtils.fromString("invalid JSON document"));
                }
                return this.currentJsonNode;
            } catch (IOException e) {
                throw ErrorCreator.createError(StringUtils.fromString("Error reading JSON: " + e.getMessage()));
            } catch (StreamParserException e) {
                throw ErrorCreator.createError(StringUtils.fromString(e.getMessage() + " at line: " + this.line + " " +
                        "column: " + this.column));
            }
        }

        private void append(char ch) {
            try {
                this.charBuff[this.charBuffIndex] = ch;
                this.charBuffIndex++;
            } catch (ArrayIndexOutOfBoundsException e) {
                /* this approach is faster than checking for the size by ourself */
                this.growCharBuff();
                this.charBuff[this.charBuffIndex++] = ch;
            }
        }

        private void growCharBuff() {
            char[] newBuff = new char[charBuff.length * 2];
            System.arraycopy(this.charBuff, 0, newBuff, 0, this.charBuff.length);
            this.charBuff = newBuff;
        }

        private State finalizeObject() throws StreamParserException {
            // may need to do something for immutables such as making mutable immutable
            Type targetType = this.targetTypes.remove(this.targetTypes.size() - 1);
            switch (targetType.getTag()) {
                case TypeTags.UNION_TAG:
                    List<Type> typeList = this.possibleTypes.get(this.possibleTypes.size() - 1);
                    boolean valueConstructed = false;
                    for (Type memType : typeList) {
                        switch (memType.getTag()) {
                            case TypeTags.MAP_TAG:
                                boolean mapConstructed = true;
                                BMapType mapType = (BMapType) memType;
                                Type constrainedType = TypeUtils.getImpliedType(mapType.getConstrainedType());
                                BMap<BString, Object> oldMap = (BMap<BString, Object>) this.currentJsonNode;
                                MapValueImpl<BString, Object> newMap = new MapValueImpl<>(mapType);
                                for (Map.Entry<BString, Object> fieldEntry : oldMap.entrySet()) {
                                    Object val = null;
                                    try {
                                        val = TypeConverter.convertValues(constrainedType, fieldEntry.getValue());
                                    } catch (BError e) {
                                        mapConstructed = false;
                                        break;
                                    }
                                    newMap.putForcefully(fieldEntry.getKey(), val);
                                }
                                if (mapConstructed) {
                                    this.currentJsonNode = newMap;
                                    valueConstructed = true;
                                }
                                break;
                            case TypeTags.RECORD_TYPE_TAG:
                                boolean recordConstructed = true;
                                BRecordType recordType = (BRecordType) memType;
                                BMap<BString, Object> oldRecordMap = (BMap<BString, Object>) this.currentJsonNode;
                                List<String> notProvidedFields = new ArrayList<>();
                                for (Map.Entry<String, Field> stringFieldEntry : recordType.getFields().entrySet()) {
                                    String fieldName = stringFieldEntry.getKey();
                                    BString bFieldName = StringUtils.fromString(fieldName);
                                    if (oldRecordMap.containsKey(bFieldName)) {
                                        continue;
                                    }
                                    if (SymbolFlags.isFlagOn(stringFieldEntry.getValue().getFlags(), SymbolFlags.REQUIRED)) {
                                        recordConstructed = false;
                                        break;
                                    } else {
                                        notProvidedFields.add(fieldName);
                                    }
                                }
                                BMap<BString, Object> recordValue = null;
                                if (recordConstructed) {
                                    recordValue = createRecordValueWithDefaultValues(recordType.getPackage(),
                                            recordType.getName(), notProvidedFields);
                                    Map<String, Field> fields = recordType.getFields();
                                    for (Map.Entry<BString, Object> fieldEntry : oldRecordMap.entrySet()) {
                                        BString fieldName = fieldEntry.getKey();
                                        Field field = fields.get(fieldName.getValue());
                                        Type fieldType = field == null ? recordType.restFieldType : field.getFieldType();
                                        Object val = null;
                                        try {
                                            val = TypeConverter.convertValues(fieldType, fieldEntry.getValue());
                                        } catch (BError e) {
                                            recordConstructed = false;
                                            break;
                                        }
                                        ((MapValueImpl<BString, Object>) recordValue).putForcefully(fieldEntry.getKey(),
                                                val);
                                    }
                                }
                                if (recordConstructed) {
                                    if (recordType.isReadOnly()) {
                                        recordValue.freezeDirect();
                                    }
                                    valueConstructed = true;
                                    this.currentJsonNode = recordValue;
                                }
                                break;
                            default:
                                break;
                        }
                        if (valueConstructed) {
                            break;
                        }
                    }
                    if (!valueConstructed) {
                        throw new StreamParserException("value cannot be constructed");
                    }
                    break;
                case TypeTags.MAP_TAG:
                    break;
                case TypeTags.RECORD_TYPE_TAG:
                    BRecordType recordType = (BRecordType) targetType;
                    BMap<BString, Object> constructedMap = (BMap<BString, Object>) this.currentJsonNode;
                    List<String> notProvidedFields = new ArrayList<>();
                    for (Map.Entry<String, Field> stringFieldEntry : recordType.getFields().entrySet()) {
                        String fieldName = stringFieldEntry.getKey();
                        BString bFieldName = StringUtils.fromString(fieldName);
                        if (constructedMap.containsKey(bFieldName)) {
                            continue;
                        }
                        if (SymbolFlags.isFlagOn(stringFieldEntry.getValue().getFlags(), SymbolFlags.REQUIRED)) {
                            throw new StreamParserException("missing required field '" + fieldName + "' of type '" +
                                    stringFieldEntry.getValue().getFieldType().toString() + "' in record '"
                                    + targetType + "'");
                        } else {
                            notProvidedFields.add(fieldName);
                        }
                    }
                    BMap<BString, Object> recordValue = createRecordValueWithDefaultValues(recordType.getPackage(),
                            recordType.getName(), notProvidedFields);
                    for (Map.Entry<BString, Object> fieldEntry : constructedMap.entrySet()) {
                        recordValue.populateInitialValue(fieldEntry.getKey(), fieldEntry.getValue());
                    }
                    if (recordType.isReadOnly()) {
                        recordValue.freezeDirect();
                    }
                    this.currentJsonNode = recordValue;
                    break;
                case TypeTags.ARRAY_TAG:
                    int listIndex = this.listIndices.remove(this.listIndices.size() - 1);
                    ArrayType arrayType = (ArrayType) targetType;
                    int targetSize = arrayType.getSize();
                    if (arrayType.getState() == ArrayType.ArrayState.CLOSED && targetSize > listIndex
                            && !arrayType.hasFillerValue()) {
                        throw new StreamParserException("target type" + arrayType
                                + " array size is too large, array does not have filler values");
                    }
                    break;
                case TypeTags.TUPLE_TAG:
                    int tupleListIndex = this.listIndices.remove(this.listIndices.size() - 1);
                    TupleType tupleType = (TupleType) targetType;
                    int targetTupleSize = tupleType.getTupleTypes().size();
                    if (targetTupleSize > tupleListIndex) {
                        throw new StreamParserException("target type tuple size is too large");
                    }
                    break;
                default:
                    throw new StreamParserException("unsupported type");
            }

            if (this.nodesStack.isEmpty()) {
                return DOC_END_STATE;
            }
            Object parentNode = this.nodesStack.pop();

            Type parentTargetType = this.targetTypes.get(this.targetTypes.size() - 1);
            switch (parentTargetType.getTag()) {
                case TypeTags.RECORD_TYPE_TAG:
                case TypeTags.MAP_TAG:
                    ((MapValueImpl<BString, Object>) parentNode).putForcefully(StringUtils.fromString(fieldNames.pop()),
                            currentJsonNode);
                    currentJsonNode = parentNode;
                    return FIELD_END_STATE;
                case TypeTags.ARRAY_TAG:
                    int listIndex = this.listIndices.get(this.listIndices.size() - 1);
                    ArrayType arrayType = (ArrayType) parentTargetType;
                    int targetSize = arrayType.getSize();
                    if (arrayType.getState() == ArrayType.ArrayState.CLOSED && targetSize <= listIndex) {
                        throw new StreamParserException("target type " + parentTargetType + " array size is not enough");
                    }
                    // **** NEED TO DO INDEX CHECKING (done), type checking (do I? think no need) AND FIX IMMUTABLE CHECKING
                    ((ArrayValueImpl) parentNode).addRefValue(listIndex, currentJsonNode);
                    this.listIndices.set(this.listIndices.size() - 1, listIndex + 1);
                    currentJsonNode = parentNode;
                    // here need to check for tuples and array whether there are enough elements upto the size
                    // eg [1] given type int[4] or [int, int]
                    return ARRAY_ELEMENT_END_STATE;
                case TypeTags.TUPLE_TAG:
                    int tupleListIndex = this.listIndices.get(this.listIndices.size() - 1);
                    TupleType tupleType = (TupleType) parentTargetType;
                    if ((tupleType.getTupleTypes().size() <= tupleListIndex) && (tupleType.getRestType() == null)) {
                        throw new StreamParserException("heee target type tuple size is not enough");
                    }
                    // **** NEED TO DO INDEX CHECKING AND FIX IMMUTABLE CHECKING
                    ((TupleValueImpl) parentNode).addRefValue(tupleListIndex, currentJsonNode);
                    this.listIndices.set(this.listIndices.size() - 1, tupleListIndex + 1);
                    currentJsonNode = parentNode;
                    // here need to check for tuples and array whether there are enough elements upto the size
                    // eg [1] given type int[4] or [int, int]
                    return ARRAY_ELEMENT_END_STATE;
                default:
                    throw new StreamParserException("unsupported type");
            }
        }

        private State initNewObject() throws StreamParserException {
            if (currentJsonNode != null) {
                this.nodesStack.push(currentJsonNode);
                Type lastTargetType = this.targetTypes.get(this.targetTypes.size() - 1);
                switch (lastTargetType.getTag()) {
                    case TypeTags.ARRAY_TAG:
                        int listIndex = this.listIndices.get(this.listIndices.size() - 1);
                        ArrayType arrayType = (ArrayType) lastTargetType;
                        int targetSize = arrayType.getSize();
                        if (arrayType.getState() == ArrayType.ArrayState.CLOSED && targetSize <= listIndex) {
                            throw new StreamParserException("target type array size is not enough");
                        }
                        Type elementType = TypeUtils.getImpliedType(arrayType.getElementType());
                        this.addTargetType(elementType);
                        break;
                    case TypeTags.TUPLE_TAG:
                        int tupleListIndex = this.listIndices.get(this.listIndices.size() - 1);
                        TupleType tupleType = (TupleType) lastTargetType;
                        List<Type> tupleTypes = tupleType.getTupleTypes();
                        int targetTupleSize = tupleTypes.size();
                        Type tupleRestType = tupleType.getRestType();
                        boolean noRestType = tupleRestType == null;
                        Type tupleElementType;
                        if (targetTupleSize <= tupleListIndex) {
                            if (noRestType) {
                                throw new StreamParserException("ho ho target type tuple size is not enough");
                            } else {
                                tupleElementType = TypeUtils.getImpliedType(tupleRestType);
                            }
                        } else {
                            tupleElementType = TypeUtils.getImpliedType(tupleTypes.get(tupleListIndex));
                        }
                        // need to add the rest type
                        // do i need to increment the list index here? or in the above array
                        this.addTargetType(tupleElementType);
                        break;
                    case TypeTags.MAP_TAG:
                        this.addTargetType(((MapType) lastTargetType).getConstrainedType());
                        break;
                    case TypeTags.RECORD_TYPE_TAG:
                        BRecordType recordType = (BRecordType) lastTargetType;
                        String fieldName = this.fieldNames.getFirst();
                        Map<String, Field> fields = recordType.getFields();
                        Field field = fields.get(fieldName);
                        if (field == null) {
                            this.addTargetType(recordType.restFieldType);
                        } else {
                            this.addTargetType(field.getFieldType());
                        }
                        break;
                    case TypeTags.UNION_TAG:
                        List<Type> typeList = this.possibleTypes.get(this.possibleTypes.size() - 1);
                        List<Type> newTypeList = new ArrayList<>();
                        Set<Type> newTypeListSet = new HashSet<>();
                        for (Type memType : typeList) {
                            switch (memType.getTag()) {
                                case TypeTags.MAP_TAG:
                                    addNewType(newTypeList, newTypeListSet, ((MapType) memType).getConstrainedType());
                                    break;
                                case TypeTags.RECORD_TYPE_TAG:
                                    recordType = (BRecordType) memType;
                                    fieldName = this.fieldNames.getFirst();
                                    fields = recordType.getFields();
                                    field = fields.get(fieldName);
                                    if (field == null) {
                                        addNewType(newTypeList, newTypeListSet, recordType.restFieldType);
                                    } else {
                                        addNewType(newTypeList, newTypeListSet, field.getFieldType());
                                    }
                                    break;
                                case TypeTags.ARRAY_TAG:
                                    listIndex = this.listIndices.get(this.listIndices.size() - 1);
                                    arrayType = (ArrayType) memType;
                                    targetSize = arrayType.getSize();
                                    if (arrayType.getState() != ArrayType.ArrayState.CLOSED || targetSize > listIndex) {
                                        elementType = TypeUtils.getImpliedType(arrayType.getElementType());
                                        addNewType(newTypeList, newTypeListSet, elementType);
                                    }
                                    break;
                                case TypeTags.TUPLE_TAG:
                                    tupleListIndex = this.listIndices.get(this.listIndices.size() - 1);
                                    tupleType = (TupleType) memType;
                                    tupleTypes = tupleType.getTupleTypes();
                                    targetTupleSize = tupleTypes.size();
                                    tupleRestType = tupleType.getRestType();
                                    if (targetTupleSize <= tupleListIndex) {
                                        if (tupleRestType != null) {
                                            addNewType(newTypeList, newTypeListSet, TypeUtils.getImpliedType(tupleRestType));
                                        }
                                    } else {
                                        addNewType(newTypeList, newTypeListSet, TypeUtils.getImpliedType(tupleTypes.get(tupleListIndex)));
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }
                        if (newTypeListSet.isEmpty()) {
                            throw new StreamParserException("no eligible type");
                        } else {
                            this.targetTypes.add(new BUnionType(newTypeList));
                        }
                        break;
                    default:
                        throw new StreamParserException("not supported initNewArray1sdxed");
                }
            }
            Type targetType = this.targetTypes.get(this.targetTypes.size() - 1);
            switch (targetType.getTag()) {
                case TypeTags.MAP_TAG:
                case TypeTags.RECORD_TYPE_TAG:
                    this.currentJsonNode = new MapValueImpl<>(targetType);
                    break;
                case TypeTags.UNION_TAG:
                    BUnionType unionType = (BUnionType) targetType;
                    List<Type> flattenedTypes = getFlattenedMemberTypes(unionType);
                    List<Type> mapAndRecordTypes = getMapAndRecordTypes(flattenedTypes);
                    if (mapAndRecordTypes.isEmpty()) {
                        throw new StreamParserException("target union type does not contain map or record type");
                    }
                    this.currentJsonNode = new MapValueImpl<>(new BMapType(this.definedJsonType));
                    this.possibleTypes.add(mapAndRecordTypes);
                    break;
                default:
                    throw new StreamParserException("target type is not map or record type");

            }
            return FIRST_FIELD_READY_STATE;
        }

        private void addNewType(List<Type> newTypeList, Set<Type> newTypeListSet, Type typeAdded) {
            if (!newTypeListSet.contains(typeAdded)) {
                newTypeList.add(typeAdded);
                newTypeListSet.add(typeAdded);
            }
        }

        private List<Type> getMapAndRecordTypes(List<Type> typeList) {
            List<Type> mapAndRecordTypes = new ArrayList<>();
            for (Type type : typeList) {
                switch (type.getTag()) {
                    case TypeTags.MAP_TAG:
                    case TypeTags.RECORD_TYPE_TAG:
                        mapAndRecordTypes.add(type);
                        break;
                    default:
                        break;
                }
            }
            return mapAndRecordTypes;
        }

        private List<Type> getFlattenedMemberTypes(BUnionType unionType) {
            List<Type> memberTypes = new ArrayList<>();
            for (Type memberType : unionType.getMemberTypes()) {
                Type impliedType = TypeUtils.getImpliedType(memberType);
                if (impliedType.getTag() == TypeTags.UNION_TAG) {
                    memberTypes.addAll(getFlattenedMemberTypes((BUnionType) impliedType));
                } else {
                    memberTypes.add(impliedType);
                }
            }
            return memberTypes;
        }

        private State initNewArray() throws StreamParserException {
            if (currentJsonNode != null) {
                this.nodesStack.push(currentJsonNode);
                Type lastTargetType = this.targetTypes.get(this.targetTypes.size() - 1);
                switch (lastTargetType.getTag()) {
                    case TypeTags.ARRAY_TAG:
                        int listIndex = this.listIndices.get(this.listIndices.size() - 1);
                        ArrayType arrayType = (ArrayType) lastTargetType;
                        int targetSize = arrayType.getSize();
                        if (arrayType.getState() == ArrayType.ArrayState.CLOSED && targetSize <= listIndex) {
                            throw new StreamParserException("target type array size is not enough");
                        }
                        Type elementType = TypeUtils.getImpliedType(arrayType.getElementType());
                        this.addTargetType(elementType);
                        break;
                    case TypeTags.TUPLE_TAG:
                        int tupleListIndex = this.listIndices.get(this.listIndices.size() - 1);
                        TupleType tupleType = (TupleType) lastTargetType;
                        List<Type> tupleTypes = tupleType.getTupleTypes();
                        int targetTupleSize = tupleTypes.size();
                        Type tupleRestType = tupleType.getRestType();
                        boolean noRestType = tupleRestType == null;
                        Type tupleElementType;
                        if (targetTupleSize <= tupleListIndex) {
                            if (noRestType) {
                                throw new StreamParserException("ho ho target type tuple size is not enough");
                            } else {
                                tupleElementType = TypeUtils.getImpliedType(tupleRestType);
                            }
                        } else {
                            tupleElementType = TypeUtils.getImpliedType(tupleTypes.get(tupleListIndex));
                        }
                        // need to add the rest type
                        // do i need to increment the list index here? or in the above array
                        this.addTargetType(tupleElementType);
                        break;
                    case TypeTags.MAP_TAG:
                        this.addTargetType(((MapType) lastTargetType).getConstrainedType());
                        break;
                    case TypeTags.RECORD_TYPE_TAG:
                        BRecordType recordType = (BRecordType) lastTargetType;
                        String fieldName = this.fieldNames.getFirst();
                        Map<String, Field> fields = recordType.getFields();
                        Field field = fields.get(fieldName);
                        if (field == null) {
                            this.addTargetType(recordType.restFieldType);
                        } else {
                            this.addTargetType(field.getFieldType());
                        }
                        break;
                    default:
                        throw new StreamParserException("not supported initNewArray1sdxed");
                }
            }
            Type targetType = this.targetTypes.get(this.targetTypes.size() - 1);
            switch (targetType.getTag()) {
                case TypeTags.ARRAY_TAG:
                    currentJsonNode = new ArrayValueImpl((ArrayType) targetType);
                    //currently can do this, later check for immutable types
                    this.listIndices.add(0);
                    return FIRST_ARRAY_ELEMENT_READY_STATE;
                case TypeTags.TUPLE_TAG:
                    currentJsonNode = new TupleValueImpl((TupleType) targetType);
                    //currently can do this, later check for immutable types
                    this.listIndices.add(0);
                    return FIRST_ARRAY_ELEMENT_READY_STATE;
                default:
                    throw new StreamParserException("target type is not array type");

            }
        }

        /**
         * A specific state in the JSON parsing state machine.
         */
        private interface State {

            /**
             * Input given to the current state for a transition.
             *
             * @param sm the state machine
             * @param buff the input characters for the current state
             * @param i the location from the character should be read from
             * @param count the number of characters to read from the buffer
             * @return the new resulting state
             */
            State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException;

        }

        /**
         * Represents the JSON document start state.
         */
        private static class DocumentStartState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                char ch;
                State state = null;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (ch == '{') {
                        state = sm.initNewObject();
                    } else if (ch == '[') {
                        state = sm.initNewArray();
                    } else if (StateMachine.isWhitespace(ch)) {
                        state = this;
                        continue;
                    } else if (ch == QUOTES) {
                        sm.currentQuoteChar = ch;
                        state = STRING_VALUE_STATE;
                    } else if (ch == EOF) {
                        throw new StreamParserException("empty JSON document");
                    } else {
                        state = NON_STRING_VALUE_STATE;
                    }
                    break;
                }
                if (state == NON_STRING_VALUE_STATE) {
                    sm.index = i;
                } else {
                    sm.index = i + 1;
                }
                return state;
            }

        }

        /**
         * Represents the JSON document end state.
         */
        private static class DocumentEndState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                char ch;
                State state = null;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (StateMachine.isWhitespace(ch) || ch == EOF) {
                        state = this;
                        continue;
                    }
                    throw new StreamParserException("JSON document has already ended");
                }
                sm.index = i + 1;
                return state;
            }

        }

        /**
         * Represents the state just before the first object field is defined.
         */
        private static class FirstFieldReadyState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                char ch;
                State state = null;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (ch == QUOTES) {
                        state = FIELD_NAME_STATE;
                        sm.currentQuoteChar = ch;
                    } else if (StateMachine.isWhitespace(ch)) {
                        state = this;
                        continue;
                    } else if (ch == '}') {
                        state = sm.finalizeObject();
                    } else {
                        StateMachine.throwExpected("\"", "}");
                    }
                    break;
                }
                sm.index = i + 1;
                return state;
            }

        }

        /**
         * Represents the state just before the first array element is defined.
         */
        private static class FirstArrayElementReadyState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (StateMachine.isWhitespace(ch)) {
                        state = this;
                        continue;
                    } else if (ch == QUOTES) {
                        state = STRING_ARRAY_ELEMENT_STATE;
                        sm.currentQuoteChar = ch;
                    } else if (ch == '{') {
                        state = sm.initNewObject();
                    } else if (ch == '[') {
                        state = sm.initNewArray();
                    } else if (ch == ']') {
                        state = sm.finalizeObject();
                    } else {
                        state = NON_STRING_ARRAY_ELEMENT_STATE;
                    }
                    break;
                }
                if (state == NON_STRING_ARRAY_ELEMENT_STATE) {
                    sm.index = i;
                } else {
                    sm.index = i + 1;
                }
                return state;
            }

        }

        /**
         * Represents the state just before a non-first object field is defined.
         */
        private static class NonFirstFieldReadyState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (ch == QUOTES) {
                        sm.currentQuoteChar = ch;
                        state = FIELD_NAME_STATE;
                    } else if (StateMachine.isWhitespace(ch)) {
                        state = this;
                        continue;
                    } else {
                        StateMachine.throwExpected("\"");
                    }
                    break;
                }
                sm.index = i + 1;
                return state;
            }

        }

        /**
         * Represents the state just before a non-first array element is defined.
         */
        private static class NonFirstArrayElementReadyState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (StateMachine.isWhitespace(ch)) {
                        state = this;
                        continue;
                    } else if (ch == QUOTES) {
                        state = STRING_ARRAY_ELEMENT_STATE;
                        sm.currentQuoteChar = ch;
                    } else if (ch == '{') {
                        state = sm.initNewObject();
                    } else if (ch == '[') {
                        state = sm.initNewArray();
                    } else if (ch == ']') {
                        throw new StreamParserException("expected an array element");
                    } else {
                        state = NON_STRING_ARRAY_ELEMENT_STATE;
                    }
                    break;
                }
                if (state == NON_STRING_ARRAY_ELEMENT_STATE) {
                    sm.index = i;
                } else {
                    sm.index = i + 1;
                }
                return state;
            }

        }

        private String value() {
            String result = new String(this.charBuff, 0, this.charBuffIndex);
            this.charBuffIndex = 0;
            return result;
        }

        private void addFieldName() {
            this.fieldNames.push(this.value());
        }

        /**
         * Represents the state during a field name.
         */
        private static class FieldNameState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                char ch;
                State state = null;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (ch == sm.currentQuoteChar) {
                        sm.addFieldName();
                        Type parentTargetType = sm.targetTypes.get(sm.targetTypes.size() - 1);
                        switch (parentTargetType.getTag()) {
                            case TypeTags.RECORD_TYPE_TAG:
                                BRecordType recordType = (BRecordType) parentTargetType;
                                String fieldName = sm.fieldNames.getFirst();
                                Map<String, Field> fields = recordType.getFields();
                                Field field = fields.get(fieldName);
                                if (field == null && recordType.sealed) {
                                    throw new StreamParserException("field '" + fieldName + "' cannot be added to the closed record '" +
                                            recordType + "'");
                                }
                                break;
                            case TypeTags.UNION_TAG:
                                List<Type> eligibleTypes = sm.possibleTypes.get(sm.possibleTypes.size() - 1);
                                List<Type> newEligibleTypes = new ArrayList<>();
                                for (Type eligibleType : eligibleTypes) {
                                    switch (eligibleType.getTag()) {
                                        case TypeTags.MAP_TAG:
                                            newEligibleTypes.add(eligibleType);
                                            break;
                                        case TypeTags.RECORD_TYPE_TAG:
                                            recordType = (BRecordType) eligibleType;
                                            fieldName = sm.fieldNames.getFirst();
                                            fields = recordType.getFields();
                                            field = fields.get(fieldName);
                                            if (field != null || !recordType.sealed) {
                                                newEligibleTypes.add(eligibleType);
                                            }
                                            break;
                                        default:
                                            break;
                                    }
                                }
                                if (newEligibleTypes.isEmpty()) {
                                    throw new StreamParserException("no eligible types");
                                } else {
                                    sm.possibleTypes.set(sm.possibleTypes.size() - 1, newEligibleTypes);
                                }
                                break;
                            default:
                                // maps can have any field name
                                break;
                        }
                        state = END_FIELD_NAME_STATE;
                    } else if (ch == REV_SOL) {
                        state = FIELD_NAME_ESC_CHAR_PROCESSING_STATE;
                    } else if (ch == EOF) {
                        throw new StreamParserException("unexpected end of JSON document");
                    } else {
                        sm.append(ch);
                        state = this;
                        continue;
                    }
                    break;
                }
                sm.index = i + 1;
                return state;
            }

        }

        /**
         * Represents the state where a field name definition has ended.
         */
        private static class EndFieldNameState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (StateMachine.isWhitespace(ch)) {
                        state = this;
                        continue;
                    } else if (ch == ':') {
                        state = FIELD_VALUE_READY_STATE;
                    } else {
                        StateMachine.throwExpected(":");
                    }
                    break;
                }
                sm.index = i + 1;
                return state;
            }

        }

        /**
         * Represents the state where a field value is about to be defined.
         */
        private static class FieldValueReadyState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (StateMachine.isWhitespace(ch)) {
                        state = this;
                        continue;
                    } else if (ch == QUOTES) {
                        state = STRING_FIELD_VALUE_STATE;
                        sm.currentQuoteChar = ch;
                    } else if (ch == '{') {
                        state = sm.initNewObject();
                    } else if (ch == '[') {
                        state = sm.initNewArray();
                    } else if (ch == ']' || ch == '}') {
                        throw new StreamParserException("expected a field value");
                    } else {
                        state = NON_STRING_FIELD_VALUE_STATE;
                    }
                    break;
                }
                if (state == NON_STRING_FIELD_VALUE_STATE) {
                    sm.index = i;
                } else {
                    sm.index = i + 1;
                }
                return state;
            }

        }

        /**
         * Represents the state during a string field value is defined.
         */
        private static class StringFieldValueState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (ch == sm.currentQuoteChar) {
                        // TODO: write for record types later
                        Type targetType = sm.targetTypes.get(sm.targetTypes.size() - 1);
                        switch (targetType.getTag()) {
                            case TypeTags.MAP_TAG:
                                if (!TypeChecker.checkIsType(((MapType) targetType).getConstrainedType(),
                                        PredefinedTypes.TYPE_STRING)) {
                                    throw new StreamParserException("map<string> expected, not a string");
                                }
                                break;
                            case TypeTags.RECORD_TYPE_TAG:
                                // in records, when processing the field name, target type is added.
                                BRecordType recordType = (BRecordType) targetType;
                                String fieldName = sm.fieldNames.getFirst();
                                Map<String, Field> fields = recordType.getFields();
                                Field field = fields.get(fieldName);
                                Type fieldType = field == null ? recordType.restFieldType : field.getFieldType();
                                if (!TypeChecker.checkIsType(fieldType, PredefinedTypes.TYPE_STRING)) {
                                    throw new StreamParserException("record rest field not a string");
                                }
                                break;
                            default:
                                throw new StreamParserException("not a map<string>");
                        }
                        ((MapValueImpl<BString, Object>) sm.currentJsonNode).putForcefully(
                                StringUtils.fromString(sm.fieldNames.pop()), StringUtils.fromString(sm.value()));
                        state = FIELD_END_STATE;
                    } else if (ch == REV_SOL) {
                        state = STRING_FIELD_ESC_CHAR_PROCESSING_STATE;
                    } else if (ch == EOF) {
                        throw new StreamParserException("unexpected end of JSON document");
                    } else {
                        sm.append(ch);
                        state = this;
                        continue;
                    }
                    break;
                }
                sm.index = i + 1;
                return state;
            }

        }

        /**
         * Represents the state during a string array element is defined.
         */
        private static class StringArrayElementState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (ch == sm.currentQuoteChar) {
                        int listIndex = sm.listIndices.get(sm.listIndices.size() - 1);
                        Type targetType = sm.targetTypes.get(sm.targetTypes.size() - 1);
                        switch (targetType.getTag()) {
                            case TypeTags.ARRAY_TAG:
                                ArrayType arrayType = (ArrayType) targetType;
                                int targetSize = arrayType.getSize();
                                if (arrayType.getState() == ArrayType.ArrayState.CLOSED && targetSize <= listIndex) {
                                    throw new StreamParserException("target type array size is not enough");
                                }
                                if (!TypeChecker.checkIsType(arrayType.getElementType(),
                                        PredefinedTypes.TYPE_STRING)) {
                                    throw new StreamParserException("given is a string, but array element type is not string");
                                }
                                ((ArrayValueImpl) sm.currentJsonNode).addRefValue(listIndex, StringUtils.fromString(sm.value()));
                                break;
                            case TypeTags.TUPLE_TAG:
                                TupleType tupleType = (TupleType) targetType;
                                List<Type> tupleTypes = tupleType.getTupleTypes();
                                int targetTupleSize = tupleTypes.size();
                                Type tupleRestType = tupleType.getRestType();
                                boolean noRestType = tupleRestType == null;
                                Type tupleElementType;
                                if (targetTupleSize <= listIndex) {
                                    if (noRestType) {
                                        throw new StreamParserException("target type tuple size is not enough");
                                    } else {
                                        tupleElementType = TypeUtils.getImpliedType(tupleRestType);
                                    }
                                } else {
                                    tupleElementType = TypeUtils.getImpliedType(tupleTypes.get(listIndex));
                                }
                                if (!TypeChecker.checkIsType(tupleElementType, PredefinedTypes.TYPE_STRING)) {
                                    throw new StreamParserException("string is given, but the tuple element type is not string");
                                }
                                ((TupleValueImpl) sm.currentJsonNode).addRefValue(listIndex, StringUtils.fromString(sm.value()));
                                break;
                            default:
                                throw new StreamParserException("string in the list expected, not a string");
                        }
                        sm.listIndices.set(sm.listIndices.size() - 1, listIndex + 1);
                        state = ARRAY_ELEMENT_END_STATE;
                    } else if (ch == REV_SOL) {
                        state = STRING_AE_ESC_CHAR_PROCESSING_STATE;
                    } else if (ch == EOF) {
                        throw new StreamParserException("unexpected end of JSON document");
                    } else {
                        sm.append(ch);
                        state = this;
                        continue;
                    }
                    break;
                }
                sm.index = i + 1;
                return state;
            }

        }

        /**
         * Represents the state during a non-string field value is defined.
         */
        private static class NonStringFieldValueState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (ch == '{') {
                        state = sm.initNewObject();
                    } else if (ch == '[') {
                        state = sm.initNewArray();
                    } else if (ch == '}' || ch == ']') {
                        sm.processNonStringValue(ValueType.FIELD);
                        state = sm.finalizeObject();
                    } else if (ch == ',') {
                        sm.processNonStringValue(ValueType.FIELD);
                        state = NON_FIRST_FIELD_READY_STATE;
                    } else if (StateMachine.isWhitespace(ch)) {
                        sm.processNonStringValue(ValueType.FIELD);
                        state = FIELD_END_STATE;
                    } else if (ch == EOF) {
                        throw new StreamParserException("unexpected end of JSON document");
                    } else {
                        sm.append(ch);
                        state = this;
                        continue;
                    }
                    break;
                }
                sm.index = i + 1;
                return state;
            }

        }

        /**
         * Represents the state during a non-string array element is defined.
         */
        private static class NonStringArrayElementState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (ch == '{') {
                        state = sm.initNewObject();
                    } else if (ch == '[') {
                        state = sm.initNewArray();
                    } else if (ch == ']') {
                        sm.processNonStringValue(ValueType.ARRAY_ELEMENT);
                        state = sm.finalizeObject();
                    } else if (ch == ',') {
                        sm.processNonStringValue(ValueType.ARRAY_ELEMENT);
                        state = NON_FIRST_ARRAY_ELEMENT_READY_STATE;
                    } else if (StateMachine.isWhitespace(ch)) {
                        sm.processNonStringValue(ValueType.ARRAY_ELEMENT);
                        state = ARRAY_ELEMENT_END_STATE;
                    } else if (ch == EOF) {
                        throw new StreamParserException("unexpected end of JSON document");
                    } else {
                        sm.append(ch);
                        state = this;
                        continue;
                    }
                    break;
                }
                sm.index = i + 1;
                return state;
            }

        }

        /**
         * Represents the state during a string value is defined.
         */
        private static class StringValueState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (ch == sm.currentQuoteChar) {
                        if (!TypeChecker.checkIsType(sm.targetTypes.get(sm.targetTypes.size() - 1),
                                PredefinedTypes.TYPE_STRING)) {
                            throw new StreamParserException("not a string, string expected");
                        }
                        sm.currentJsonNode = StringUtils.fromString(sm.value());
                        state = DOC_END_STATE;
                    } else if (ch == REV_SOL) {
                        state = STRING_VAL_ESC_CHAR_PROCESSING_STATE;
                    } else if (ch == EOF) {
                        throw new StreamParserException("unexpected end of JSON document");
                    } else {
                        sm.append(ch);
                        state = this;
                        continue;
                    }
                    break;
                }
                sm.index = i + 1;
                return state;
            }

        }

        private enum ValueType {
            FIELD, VALUE, ARRAY_ELEMENT
        }

        private void processNonStringValue(ValueType type) throws StreamParserException {
            String str = value();
            Type targetType = this.targetTypes.get(this.targetTypes.size() - 1);
            Type referredType = TypeUtils.getImpliedType(targetType);
            switch (referredType.getTag()) {
                case TypeTags.UNION_TAG:
                    List<Type> typeList = this.possibleTypes.get(this.possibleTypes.size() - 1);
                    List<Type> newEligibleTypes = new ArrayList<>();
                    switch (type) {
                        case VALUE:
                            for (Type memType : typeList) {
                                try {
                                    this.currentJsonNode = convertValues(memType, str);
                                    return;
                                } catch (StreamParserException ignored) {

                                }
                            }
                            throw new StreamParserException("no matching typeeeee");
                        case ARRAY_ELEMENT:
                            int listIndex = this.listIndices.get(this.listIndices.size() - 1);
                            for (Type memType : typeList) {
                                switch (memType.getTag()) {
                                    case TypeTags.ARRAY_TAG:
                                        ArrayType arrayType = (ArrayType) memType;
                                        int targetSize = arrayType.getSize();
                                        if (arrayType.getState() != ArrayType.ArrayState.CLOSED || targetSize > listIndex) {
                                            newEligibleTypes.add(memType);
                                        }
                                        break;
                                    case TypeTags.TUPLE_TAG:
                                        TupleType tupleType = (TupleType) memType;
                                        List<Type> tupleTypes = tupleType.getTupleTypes();
                                        if (tupleTypes.size() <= listIndex) {
                                            if (tupleType.getRestType() != null) {
                                                newEligibleTypes.add(memType);
                                            }
                                        } else {
                                            newEligibleTypes.add(memType);
                                        }
                                        break;
                                    default:
                                        break;
                                }
                            }
                            // add the element to arrrayvlimpl
                            Object jsonVal = processNonStringValueAsJson(str);
                            ((ArrayValueImpl) this.currentJsonNode).addRefValue(listIndex, jsonVal);
                            this.listIndices.set(this.listIndices.size() - 1, listIndex + 1);
                            break;
                        default:
                            // default is field
                            for (Type memType : typeList) {
                                switch (memType.getTag()) {
                                    case TypeTags.MAP_TAG:
                                    case TypeTags.RECORD_TYPE_TAG:
                                        // no problem, add to map val impl
                                        newEligibleTypes.add(memType);
                                        break;
                                    default:
                                        break;
                                }
                            }
                            jsonVal = processNonStringValueAsJson(str);
                            ((MapValueImpl<BString, Object>) this.currentJsonNode).putForcefully(
                                    StringUtils.fromString(this.fieldNames.pop()), jsonVal);
                            break;
                    }
                    if (newEligibleTypes.isEmpty()) {
                        throw new StreamParserException("no eligible types");
                    }
                    break;
                case TypeTags.ARRAY_TAG:
                    if (type != ValueType.ARRAY_ELEMENT) {
                        throw new StreamParserException("target type is not array type 887");
                    }
                    int listIndex = this.listIndices.get(this.listIndices.size() - 1);
                    ArrayType arrayType = (ArrayType) referredType;
                    int targetSize = arrayType.getSize();
                    if (arrayType.getState() == ArrayType.ArrayState.CLOSED && targetSize <= listIndex) {
                        throw new StreamParserException("target type array size is not enough");
                    }
                    Type elementType = TypeUtils.getImpliedType(arrayType.getElementType());
                    // following method will convert the strings to basic types
                    ((ArrayValueImpl) this.currentJsonNode).addRefValue(listIndex, convertValues(elementType, str));
                    this.listIndices.set(this.listIndices.size() - 1, listIndex + 1);
                    break;
                case TypeTags.TUPLE_TAG:
                    if (type != ValueType.ARRAY_ELEMENT) {
                        // these may be not needed, remove later
                        throw new StreamParserException("target type is not tuple type 887");
                    }
                    int tupleListIndex = this.listIndices.get(this.listIndices.size() - 1);
                    TupleType tupleType = (TupleType) referredType;
                    List<Type> tupleTypes = tupleType.getTupleTypes();
                    int targetTupleSize = tupleTypes.size();
                    Type tupleRestType = tupleType.getRestType();
                    boolean noRestType = tupleRestType == null;
                    Type tupleElementType;
                    if (targetTupleSize <= tupleListIndex) {
                        if (noRestType) {
                            throw new StreamParserException("target type tuple size is not enough");
                        } else {
                            tupleElementType = TypeUtils.getImpliedType(tupleRestType);
                        }
                    } else {
                        tupleElementType = TypeUtils.getImpliedType(tupleTypes.get(tupleListIndex));
                    }
                    // following method will convert the strings to basic types
                    ((TupleValueImpl) this.currentJsonNode).addRefValue(tupleListIndex,
                            convertValues(tupleElementType, str));
                    this.listIndices.set(this.listIndices.size() - 1, tupleListIndex + 1);
                    break;
                case TypeTags.MAP_TAG:
                    if (type == ValueType.ARRAY_ELEMENT) {
                        throw new StreamParserException("target type is not array type 887");
                    }
                    MapType mapType = (MapType) referredType;
                    Type constrainedType = TypeUtils.getImpliedType(mapType.getConstrainedType());
                    // following method will convert the strings to basic types
                    ((MapValueImpl<BString, Object>) this.currentJsonNode).putForcefully(
                            StringUtils.fromString(this.fieldNames.pop()), convertValues(constrainedType,
                                    str));
                    break;
                case TypeTags.RECORD_TYPE_TAG:
                    BRecordType recordType = (BRecordType) referredType;
                    String fieldName = this.fieldNames.pop();
                    Map<String, Field> fields = recordType.getFields();
                    Field field = fields.get(fieldName);
                    Type fieldType = field == null ? recordType.restFieldType : field.getFieldType();
                    // following method will convert the strings to basic types
                    ((MapValueImpl<BString, Object>) this.currentJsonNode).putForcefully(
                            StringUtils.fromString(fieldName), convertValues(TypeUtils.getImpliedType(fieldType), str));
                    break;
                default:
                    this.currentJsonNode = convertValues(referredType, str);
            }
        }

        private Object processNonStringValueAsJson(String str) throws StreamParserException {
            if (str.indexOf('.') >= 0) {
                try {
                    if (isNegativeZero(str)) {
                        return Double.parseDouble(str);
                    } else {
                        return new DecimalValue(str);
                    }
                } catch (NumberFormatException ignore) {
                    throw new StreamParserException("unrecognized token '" + str + "'");
                }
            } else {
                char ch = str.charAt(0);
                if (ch == 't' && TRUE.equals(str)) {
                    return Boolean.TRUE;
                } else if (ch == 'f' && FALSE.equals(str)) {
                    return Boolean.FALSE;
                } else if (ch == 'n' && NULL.equals(str)) {
                    return null;
                } else {
                    try {
                        if (isNegativeZero(str)) {
                            return Double.parseDouble(str);
                        } else if (isExponential(str)) {
                            return new DecimalValue(str);
                        } else {
                            return Long.parseLong(str);
                        }
                    } catch (NumberFormatException ignore) {
                        throw new StreamParserException("unrecognized token '" + str + "'");
                    }
                }
            }
        }

//        private void processNonStringValue(ValueType type) throws StreamParserException {
//            String str = value();
//            if (str.indexOf('.') >= 0) {
//                try {
//                    if (isNegativeZero(str)) {
//                        setValueToJsonType(type, Double.parseDouble(str));
//                    } else {
//                        setValueToJsonType(type, new DecimalValue(str));
//                    }
//                } catch (NumberFormatException ignore) {
//                    throw new StreamParserException("unrecognized token '" + str + "'");
//                }
//            } else {
//                char ch = str.charAt(0);
//                if (ch == 't' && TRUE.equals(str)) {
//                    switch (type) {
//                        case ARRAY_ELEMENT:
//                            ((ArrayValue) this.currentJsonNode).append(Boolean.TRUE);
//                            break;
//                        case FIELD:
//                            ((MapValueImpl<BString, Object>) this.currentJsonNode).put(
//                                    StringUtils.fromString(this.fieldNames.pop()), Boolean.TRUE);
//                            break;
//                        case VALUE:
//                            currentJsonNode = Boolean.TRUE;
//                            break;
//                        default:
//                            break;
//                    }
//                } else if (ch == 'f' && FALSE.equals(str)) {
//                    switch (type) {
//                        case ARRAY_ELEMENT:
//                            ((ArrayValue) this.currentJsonNode).append(Boolean.FALSE);
//                            break;
//                        case FIELD:
//                            ((MapValueImpl<BString, Object>) this.currentJsonNode).put(
//                                    StringUtils.fromString(this.fieldNames.pop()), Boolean.FALSE);
//                            break;
//                        case VALUE:
//                            currentJsonNode = Boolean.FALSE;
//                            break;
//                        default:
//                            break;
//                    }
//                } else if (ch == 'n' && NULL.equals(str)) {
//                    switch (type) {
//                        case ARRAY_ELEMENT:
//                            ((ArrayValue) this.currentJsonNode).append(null);
//                            break;
//                        case FIELD:
//                            ((MapValueImpl<BString, Object>) this.currentJsonNode).put(
//                                    StringUtils.fromString(this.fieldNames.pop()), null);
//                            break;
//                        case VALUE:
//                            currentJsonNode = null;
//                            break;
//                        default:
//                            break;
//                    }
//                } else {
//                    try {
//                        if (isNegativeZero(str)) {
//                            setValueToJsonType(type, Double.parseDouble(str));
//                        } else if (isExponential(str)) {
//                            setValueToJsonType(type, new DecimalValue(str));
//                        } else {
//                            setValueToJsonType(type, Long.parseLong(str));
//                        }
//                    } catch (NumberFormatException ignore) {
//                        throw new StreamParserException("unrecognized token '" + str + "'");
//                    }
//                }
//            }
//        }

        private boolean isExponential(String str) {
            return str.contains("e") || str.contains("E");
        }

//        private void setValueToJsonType(ValueType type, Object value) {
//            switch (type) {
//                case ARRAY_ELEMENT:
//                    ((ArrayValue) this.currentJsonNode).append(value);
//                    break;
//                case FIELD:
//                    ((MapValueImpl<BString, Object>) this.currentJsonNode).put(
//                            StringUtils.fromString(this.fieldNames.pop()), value);
//                    break;
//                default:
//                    currentJsonNode = value;
//                    break;
//            }
//        }

        private boolean isNegativeZero(String str) {
            return '-' == str.charAt(0) && 0 == Double.parseDouble(str);
        }

        /**
         * Represents the state during a non-string value is defined.
         */
        private static class NonStringValueState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (StateMachine.isWhitespace(ch) || ch == EOF) {
                        sm.currentJsonNode = null;
                        sm.processNonStringValue(ValueType.VALUE);
                        state = DOC_END_STATE;
                    } else {
                        sm.append(ch);
                        state = this;
                        continue;
                    }
                    break;
                }
                sm.index = i + 1;
                return state;
            }

        }

        /**
         * Represents the state where an object field has ended.
         */
        private static class FieldEndState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (StateMachine.isWhitespace(ch)) {
                        state = this;
                        continue;
                    } else if (ch == ',') {
                        state = NON_FIRST_FIELD_READY_STATE;
                    } else if (ch == '}') {
                        state = sm.finalizeObject();
                    } else {
                        StateMachine.throwExpected(",", "}");
                    }
                    break;
                }
                sm.index = i + 1;
                return state;
            }

        }

        /**
         * Represents the state where an array element has ended.
         */
        private static class ArrayElementEndState implements State {

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if (StateMachine.isWhitespace(ch)) {
                        state = this;
                        continue;
                    } else if (ch == ',') {
                        state = NON_FIRST_ARRAY_ELEMENT_READY_STATE;
                    } else if (ch == ']') {
                        state = sm.finalizeObject();
                    } else {
                        StateMachine.throwExpected(",", "]");
                    }
                    break;
                }
                sm.index = i + 1;
                return state;
            }

        }

        /**
         * Represents the state where an escaped unicode character in hex format is processed
         * from a object string field.
         */
        private static class StringFieldUnicodeHexProcessingState extends UnicodeHexProcessingState {

            @Override
            protected State getSourceState() {
                return STRING_FIELD_VALUE_STATE;
            }

        }

        /**
         * Represents the state where an escaped unicode character in hex format is processed
         * from an array string field.
         */
        private static class StringAEProcessingState extends UnicodeHexProcessingState {

            @Override
            protected State getSourceState() {
                return STRING_ARRAY_ELEMENT_STATE;
            }

        }

        /**
         * Represents the state where an escaped unicode character in hex format is processed
         * from a string value.
         */
        private static class StringValueUnicodeHexProcessingState extends UnicodeHexProcessingState {

            @Override
            protected State getSourceState() {
                return STRING_VALUE_STATE;
            }

        }

        /**
         * Represents the state where an escaped unicode character in hex format is processed
         * from a field name.
         */
        private static class FieldNameUnicodeHexProcessingState extends UnicodeHexProcessingState {

            @Override
            protected State getSourceState() {
                return FIELD_NAME_STATE;
            }

        }

        /**
         * Represents the state where an escaped unicode character in hex format is processed.
         */
        private abstract static class UnicodeHexProcessingState implements State {

            protected abstract State getSourceState();

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                for (; i < count; i++) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    if ((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'F') || (ch >= 'a' && ch <= 'f')) {
                        sm.hexBuilder.append(ch);
                        if (sm.hexBuilder.length() >= 4) {
                            sm.append(this.extractUnicodeChar(sm));
                            this.reset(sm);
                            state = this.getSourceState();
                            break;
                        }
                        state = this;
                        continue;
                    }
                    this.reset(sm);
                    throw new StreamParserException("expected the hexadecimal value of a unicode character");
                }
                sm.index = i + 1;
                return state;
            }

            private void reset(StateMachine sm) {
                sm.hexBuilder.setLength(0);
            }

            private char extractUnicodeChar(StateMachine sm) {
                return StringEscapeUtils.unescapeJava("\\u" + sm.hexBuilder.toString()).charAt(0);
            }

        }

        /**
         * Represents the state where an escaped character is processed in a object string field.
         */
        private static class StringFieldEscapedCharacterProcessingState extends EscapedCharacterProcessingState {

            @Override
            protected State getSourceState() {
                return STRING_FIELD_VALUE_STATE;
            }

        }

        /**
         * Represents the state where an escaped character is processed in an array string field.
         */
        private static class StringAEEscapedCharacterProcessingState extends EscapedCharacterProcessingState {

            @Override
            protected State getSourceState() {
                return STRING_ARRAY_ELEMENT_STATE;
            }

        }

        /**
         * Represents the state where an escaped character is processed in a string value.
         */
        private static class StringValueEscapedCharacterProcessingState extends EscapedCharacterProcessingState {

            @Override
            protected State getSourceState() {
                return STRING_VALUE_STATE;
            }

        }

        /**
         * Represents the state where an escaped character is processed in a field name.
         */
        private static class FieldNameEscapedCharacterProcessingState extends EscapedCharacterProcessingState {

            @Override
            protected State getSourceState() {
                return FIELD_NAME_STATE;
            }

        }

        /**
         * Represents the state where an escaped character is processed.
         */
        private abstract static class EscapedCharacterProcessingState implements State {

            protected abstract State getSourceState();

            @Override
            public State transition(StateMachine sm, char[] buff, int i, int count) throws StreamParserException {
                State state = null;
                char ch;
                if (i < count) {
                    ch = buff[i];
                    sm.processLocation(ch);
                    switch (ch) {
                        case '"':
                            sm.append(QUOTES);
                            state = this.getSourceState();
                            break;
                        case '\\':
                            sm.append(REV_SOL);
                            state = this.getSourceState();
                            break;
                        case '/':
                            sm.append(SOL);
                            state = this.getSourceState();
                            break;
                        case 'b':
                            sm.append(BACKSPACE);
                            state = this.getSourceState();
                            break;
                        case 'f':
                            sm.append(FORMFEED);
                            state = this.getSourceState();
                            break;
                        case 'n':
                            sm.append(NEWLINE);
                            state = this.getSourceState();
                            break;
                        case 'r':
                            sm.append(CR);
                            state = this.getSourceState();
                            break;
                        case 't':
                            sm.append(HZ_TAB);
                            state = this.getSourceState();
                            break;
                        case 'u':
                            if (this.getSourceState() == STRING_FIELD_VALUE_STATE) {
                                state = STRING_FIELD_UNICODE_HEX_PROCESSING_STATE;
                            } else if (this.getSourceState() == STRING_VALUE_STATE) {
                                state = STRING_VALUE_UNICODE_HEX_PROCESSING_STATE;
                            } else if (this.getSourceState() == FIELD_NAME_STATE) {
                                state = FIELD_NAME_UNICODE_HEX_PROCESSING_STATE;
                            } else if (this.getSourceState() == STRING_ARRAY_ELEMENT_STATE) {
                                state = STRING_AE_PROCESSING_STATE;
                            } else {
                                throw new StreamParserException("unknown source '" + this.getSourceState() +
                                        "' in escape char processing state");
                            }
                            break;
                        default:
                            throw new StreamParserException("expected escaped characters");
                    }
                }
                sm.index = i + 1;
                return state;
            }

        }

    }

}
