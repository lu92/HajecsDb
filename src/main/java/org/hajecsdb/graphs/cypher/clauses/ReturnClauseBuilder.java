package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.ContentType;
import org.hajecsdb.graphs.cypher.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.DFA.DfaAction;
import org.hajecsdb.graphs.cypher.DFA.State;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.fest.util.Strings.isEmpty;


public class ReturnClauseBuilder extends ClauseBuilder {

    public ReturnClauseBuilder() {
        super(ClauseEnum.RETURN);
    }

    @Override
    public DfaAction clauseAction() {
        return new DfaAction() {
            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                Pattern pattern = Pattern.compile(getExpressionOfClauseRegex());
                Matcher matcher = pattern.matcher(commandProcessing.getClauseInvocationStack().peek().getSubQuery());
                if (matcher.find()) {
                    String variable = matcher.group(1);
                    String searchedProperty = matcher.group(2);

                    result.getResults().clear();
                    if (isEmpty(searchedProperty)) {
                        for (Map.Entry<Integer, ResultRow> entry : commandProcessing.getQueryContext().get(variable).getResults().entrySet()) {
                            result.getResults().put(entry.getKey(), entry.getValue());
                        }
                    } else {
                        for (Map.Entry<Integer, ResultRow> entry : commandProcessing.getQueryContext().get(variable).getResults().entrySet()) {

                            ResultRow row = entry.getValue(); // nasz node

                            ResultRow searchedPropertyResultRow = getSearchedProperty(row, searchedProperty);
                            result.getResults().put(entry.getKey(), searchedPropertyResultRow);
                        }
                    }
                }
                return result;
            }

            private ResultRow getSearchedProperty(ResultRow row, String searchedProperty) {
                ResultRow searchedPropertyResultRow = new ResultRow();
                Optional<Property> searchedPropertyOptional = null;
                switch (row.getContentType()) {
                    case NODE:
                        System.out.println("MATCHED NODE");
                        searchedPropertyOptional = row.getNode().getAllProperties().getProperty(searchedProperty);
                        if (searchedPropertyOptional.isPresent()) {
                            Property property = searchedPropertyOptional.get();
                            searchedPropertyResultRow = mapPropertyToResultRow(property);
                            return searchedPropertyResultRow;
                        }

                        searchedPropertyResultRow.setContentType(ContentType.NONE);
                        return searchedPropertyResultRow;

                    case RELATIONSHIP:
                        System.out.println("MATCHED RELATIONSHIP");
                        searchedPropertyOptional = row.getNode().getAllProperties().getProperty(searchedProperty);
                        if (searchedPropertyOptional.isPresent()) {
                            Property property = searchedPropertyOptional.get();
                            searchedPropertyResultRow = mapPropertyToResultRow(property);
                            return searchedPropertyResultRow;
                        }

                        searchedPropertyResultRow.setContentType(ContentType.NONE);
                        return searchedPropertyResultRow;

                    default:
                        throw new IllegalArgumentException("Type not recognized!");
                }
            }

            private ResultRow mapPropertyToResultRow(Property property) {
                ResultRow searchedPropertyResultRow = new ResultRow();
                switch (property.getType()) {
                    case INT:
                        System.out.println("MATCHED INT");
                        searchedPropertyResultRow.setContentType(ContentType.INT);
                        searchedPropertyResultRow.setIntValue((Integer) property.getValue());
                        break;

                    case LONG:
                        System.out.println("MATCHED LONG");
                        searchedPropertyResultRow.setContentType(ContentType.LONG);
                        searchedPropertyResultRow.setLongValue((Long) property.getValue());
                        break;

                    case FLOAT:
                        System.out.println("MATCHED FLOAT");
                        searchedPropertyResultRow.setContentType(ContentType.FLOAT);
                        searchedPropertyResultRow.setFloatValue((Float) property.getValue());
                        break;

                    case DOUBLE:
                        System.out.println("MATCHED DOUBLE");
                        searchedPropertyResultRow.setContentType(ContentType.DOUBLE);
                        searchedPropertyResultRow.setDoubleValue((Double) property.getValue());
                        break;

                    case STRING:
                        System.out.println("MATCHED STRING");
                        searchedPropertyResultRow.setContentType(ContentType.STRING);
                        searchedPropertyResultRow.setMessage((String) property.getValue());
                        break;
                }
                return searchedPropertyResultRow;
            }
        };


    }

    @Override
    public String getExpressionOfClauseRegex() {
        //      ([\w]+)\.?([\w]+)?
        return "([\\w]+)\\.?([\\w]+)?";
    }
}
