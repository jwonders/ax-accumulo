package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Objects;

public class Cell implements Comparable<Cell> {

    private final Key key;
    private final Value value;

    private Cell(Key key, Value value) {
        this.key = Objects.requireNonNull(key);
        this.value = Objects.requireNonNull(value);
    }

    public Key getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Cell cell = (Cell) o;
        return Objects.equals(key, cell.key) &&
                Objects.equals(value, cell.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public int compareTo(Cell cell) {
        return key.compareTo(cell.key);
    }

    public static Cell of(Key key, Value value) {
        return new Cell(key, value);
    }

    public static Cell withEmptyValue(Key key) {
        return new Cell(key, new Value());
    }

}
