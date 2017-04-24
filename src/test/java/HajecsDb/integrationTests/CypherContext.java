package HajecsDb.integrationTests;

import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableAutoConfiguration
@ComponentScan(basePackages = { "org.hajecsdb.graphs.restLayer" })
public class CypherContext
{
    @Bean
    public CypherExecutor cypherExecutor() {
        return new CypherExecutor();
    }
}