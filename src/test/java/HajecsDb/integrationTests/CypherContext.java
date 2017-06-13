package HajecsDb.integrationTests;

import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.restLayer.AbstractCluster;
import org.hajecsdb.graphs.restLayer.CoordinatorCluster;
import org.hajecsdb.graphs.restLayer.RestCommunicationProtocol;
import org.hajecsdb.graphs.restLayer.SessionPool;
import org.hajecsdb.graphs.restLayer.config.VoterConfig;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.ArrayList;

@Configuration
@EnableAutoConfiguration
@ComponentScan(basePackages = { "org.hajecsdb.graphs.restLayer" })
public class CypherContext
{
    @Bean
    public CypherExecutor cypherExecutor() {
        return new CypherExecutor();
    }

    @Bean
    public CommunicationProtocol getCommunicationProtocol() {
        return new RestCommunicationProtocol();
    }

    @Bean
    public CypherExecutor getCypherExecutor() {
        return new CypherExecutor();
    }

    @Bean
    public Environment getEnvironment() {
        Environment environment = Mockito.mock(Environment.class);
        Mockito.when(environment.getProperty("server.port")).thenReturn("7000");
        return environment;
    }

    @Bean
    public VoterConfig getCoordinatorConfig() {
        VoterConfig voterConfig = Mockito.mock(VoterConfig.class);
        Mockito.when(voterConfig.getHosts()).thenReturn(new ArrayList<HostAddress>());
        return voterConfig;
    }

    @Bean
    public SessionPool getSessionPool() {
        return new SessionPool();
    }

    @Bean
    public AbstractCluster getCoordinatorCluster() {
        return new CoordinatorCluster(getCommunicationProtocol(), getCypherExecutor(), getCoordinatorConfig(), getEnvironment());
    }
}