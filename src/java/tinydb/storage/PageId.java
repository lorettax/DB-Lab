package tinydb.storage;

public interface PageId {

    int[] serialize();

    int getTableId();

    @Override
    int hashCode();

    @Override
    boolean equals(Object o);

    int getPageNumber();
}

