package cello.introspector;

/**
 * @author Marcello Bastea-Forte (marcello@cellosoft.com)
 * @created 2010.11.22
 */
public class JavaBean {
    private String foo = "foo";
    private int four = 4;
    @TestAnnotation
    private Integer maybeFive = 5;
    private final boolean readOnly = true;

    @TestAnnotation
    public String getFoo() {
        return foo;
    }

    public void setFoo(String foo) {
        this.foo = foo;
    }

    public int getFour() {
        return four;
    }

    @TestAnnotation
    public void setFour(int four) {
        this.four = four;
    }

    public Integer getMaybeFive() {
        return maybeFive;
    }

    public void setMaybeFive(Integer maybeFive) {
        this.maybeFive = maybeFive;
    }

    public boolean isReadOnly() {
        return readOnly;
    }
}
