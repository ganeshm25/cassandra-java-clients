package com.github.jkrentz.cassandra;

import java.math.BigDecimal;
import java.util.UUID;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.joda.time.DateTime;

public class Observation {

    private final UUID procedureId;
    private final DateTime dateTime;
    private final BigDecimal value;

    public Observation(UUID procedure, DateTime timestamp, BigDecimal value) {
        this.procedureId = procedure;
        this.dateTime = timestamp;
        this.value = value;
    }

    public UUID getProcedureId() {
        return procedureId;
    }

    public DateTime getDateTime() {
        return dateTime;
    }

    public BigDecimal getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
