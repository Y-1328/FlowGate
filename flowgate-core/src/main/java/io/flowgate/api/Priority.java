package io.flowgate.api;

import java.util.Objects;

public final class Priority implements Comparable<Priority> {

    public static final Priority CRITICAL = new Priority("CRITICAL", 4);
    public static final Priority HIGH = new Priority("HIGH", 3);
    public static final Priority NORMAL = new Priority("NORMAL", 2);
    public static final Priority LOW = new Priority("LOW", 1);

    private final String name;
    private final int rank;

    public Priority(String name, int rank) {
        this.name = Objects.requireNonNull(name, "name must not be null");

        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }

        if (rank < 0) {
            throw new IllegalArgumentException("rank must be non-negative");
        }

        this.rank = rank;
    }

    public String name() {
        return name;
    }

    public int rank() {
        return rank;
    }

    @Override
    public int compareTo(Priority other) {
        Objects.requireNonNull(other, "other must not be null");
        return Integer.compare(this.rank, other.rank);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof Priority other)) {
            return false;
        }

        return rank == other.rank && name.equals(other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, rank);
    }

    @Override
    public String toString() {
        return name;
    }
}