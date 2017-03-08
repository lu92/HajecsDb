package org.hajecsdb.graphs.cypher.DFA;

import org.hajecsdb.graphs.cypher.Query;

import java.util.LinkedList;
import java.util.List;

public class CommandProcessing {
    private String command;
    private String commandToProceed;
    private List<Query> queries = new LinkedList<>();
    private List<ProcessedPartOfCommand> parts = new LinkedList<>();

    public CommandProcessing(String command) {
        this.command = command;
        this.commandToProceed = command;
    }

    public void recordProcessedPart(String part, State state) {
        parts.add(new ProcessedPartOfCommand(part, state));
    }

    public void updateCommand(String newCommand) {
        this.commandToProceed = newCommand;
    }

    public String getOriginCommand() {
        return command;
    }

    public String getProcessingCommand() {
        return commandToProceed;
    }

    public List<ProcessedPartOfCommand> getParts() {
        return parts;
    }

    public List<Query> getQueries() {
        return queries;
    }

    public class ProcessedPartOfCommand {
        private String proceedPart;
        private State state;

        public ProcessedPartOfCommand(String proceedPart, State state) {
            this.proceedPart = proceedPart;
            this.state = state;
        }

        public String getProceedPart() {
            return proceedPart;
        }

        public State getState() {
            return state;
        }
    }
}
