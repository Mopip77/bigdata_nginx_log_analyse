package myenum;

public enum VIFSortMainkey implements SortMainkey {
    VISITCOUNT("visit count", 0),
    IPCOUNT("unique ip count", 1),
    FLUXCOUNT("flux count", 2);

    private String name;
    private int key;


    VIFSortMainkey(String name, int key) {
        this.name = name;
        this.key = key;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }
}
