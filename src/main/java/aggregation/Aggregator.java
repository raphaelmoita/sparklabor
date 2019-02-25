package aggregation;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Aggregator implements Serializable {

    private String key;

    private List<Employee> employees;

    public Aggregator(String key, Iterator<Employee> employeesIter) {
        this.key = key;
        this.employees = Lists.newArrayList(employeesIter);
    }

    public List<Employee> getEmployees() {
        return employees;
    }

    public String getKey() {
        return key;

    }
}
