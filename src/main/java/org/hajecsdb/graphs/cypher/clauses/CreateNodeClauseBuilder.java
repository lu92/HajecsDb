package org.hajecsdb.graphs.cypher.clauses;


import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.cypher.clauses.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.clauses.DFA.DfaAction;
import org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateNodeClauseBuilder extends ClauseBuilder {

    public CreateNodeClauseBuilder() {
        super(ClauseEnum.CREATE_NODE);
    }

    @Override
    public DfaAction clauseAction() {

        return new DfaAction() {
            @Override
            public Result perform(TransactionalGraphService graph, Transaction transaction, Result result, CommandProcessing commandProcessing) {
                Pattern pattern = Pattern.compile(getExpressionOfClauseRegex());
                Matcher matcher = pattern.matcher(commandProcessing.getClauseInvocationStack().peek().getSubQuery());
                if (matcher.find()) {
                    String variableName = matcher.group(1);
                    Label label = new Label(matcher.group(2));
                    String parametersBody = matcher.group(4);
                    List<Property> parameters = parameterExtractor.extractParameters(parametersBody);
                    createNode(graph, transaction, result, label, parameters);
                    commandProcessing.getQueryContext().insert(variableName, result.copy());
                }
                return result;
            }

            void createNode(TransactionalGraphService graph, Transaction transaction, Result result, Label label, List<Property> parameters) {
                Properties properties = new Properties();
                properties.addAll(parameters);
                Node node = graph.context(transaction).createNode(label, properties);
                int index = result.getResults().size();
                ResultRow resultRow = new ResultRow();
                result.setCompleted(true);
                resultRow.setContentType(ContentType.NODE);
                resultRow.setNode(node);
                result.getResults().put(index, resultRow);
            }
        };
    }

    @Override
    public String getExpressionOfClauseRegex() {
        return "\\(([\\w]*):([\\w]+)( )?(\\{[\\w: ',.]*\\})?\\)";
    }
}
