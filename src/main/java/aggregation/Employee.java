package aggregation;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

public class Employee implements Serializable {

    public String name;

    public String department;

    private Employee(String name, String department) {
        this.name = name;
        this.department = department;
    }

    public static Employee create(String name, String department) {
        return new Employee(name, department);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("name", name)
                .append("department", department)
                .toString();
    }
}
