package serialization;

public class Class2
{
    public static final String A = "A";
    public static final String B = "B";
    public static final String C = "C";
    public static final String D = "D";

    public int code;

    private Class2() {}

    public static Class2 getInstance() {
        return new Class2();
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Class2{");
        sb.append("code=")
            .append(code);
        sb.append('}');
        return sb.toString();
    }
}
