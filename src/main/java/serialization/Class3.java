package serialization;

public class Class3
{
    public long id = 100L;

    private Class3() {}

    public static Class3 getInstance() {
        return new Class3();
    }


}
