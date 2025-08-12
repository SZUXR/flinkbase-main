package org.example.transformation.model;

public class CountBean {
    public String word;
    public Integer count;
    public CountBean() {}
    public CountBean(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public static CountBean of(String word, Integer count) {
        return new CountBean(word, 1);
    }
    
    @Override
    public String toString() {
        return "CountBean{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
    
    @Override
    public int hashCode() {
        return word.hashCode();
    }
}
