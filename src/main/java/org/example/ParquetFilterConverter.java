package org.example;


import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.Optional;

public class ParquetFilterConverter {
    public static Optional<FilterPredicate> toParquetFilter(Predicate deltaPredicate) {
        String predicateName = deltaPredicate.getName();
        List<Expression> children = deltaPredicate.getChildren();

        try {
            if (children.size() == 2 && children.get(0) instanceof Column && children.get(1) instanceof Literal) {
                Column column = (Column) children.get(0);
                Literal literal = (Literal) children.get(1);

                if (column.getNames().length == 0) {
                    System.err.println("Predicate column has no name.");
                    return Optional.empty();
                }
                String columnName = column.getNames()[0];

                switch (predicateName.toUpperCase()) {
                    case ">": // In Kernel, the predicate name is the symbol itself.
                    case "GREATER_THAN":
                        if (literal.getValue() instanceof Long) {
                            return Optional.of(FilterApi.gt(FilterApi.longColumn(columnName), (long) literal.getValue()));
                        }
                        if(literal.getValue() instanceof Integer) {
                            return Optional.of(FilterApi.gt(FilterApi.intColumn(columnName), (int) literal.getValue()));
                        }
                        break;
                    case "<":
                    case "LESS_THAN":
                        if (literal.getValue() instanceof Long) {
                            return Optional.of(FilterApi.lt(FilterApi.longColumn(columnName), (Long) literal.getValue()));
                        }
                        break;
                }
            }
        } catch (Exception e) {
            System.err.println("Could not convert predicate to Parquet filter. Will read full file. Error: " + e.getMessage());
            return Optional.empty();
        }

        System.err.println("Predicate " + deltaPredicate + " is too complex for this converter. Reading full file.");
        return Optional.empty();
    }
}
