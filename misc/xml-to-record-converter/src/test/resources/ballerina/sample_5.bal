type Codes record {
    (decimal|int|string)[] item;
};

@xmldata:Name {
    value: "bookstore"
}
type Bookstore record {
    string storeName;
    int postalCode;
    boolean isOpen;
    Codes codes;
    @xmldata:Attribute
    string status;
    @xmldata:Attribute
    string xmlns\:ns0 = "http://sample.com/test";
};
