package HajecsDb.integrationTests;

import HajecsDb.unitTests.utils.FileUtils;
import org.fest.assertions.Assertions;
import org.hajecsdb.graphs.IdGenerator;
import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.restLayer.ApplicationController;
import org.hajecsdb.graphs.restLayer.dto.Command;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;
import org.hajecsdb.graphs.restLayer.dto.ResultRowDto;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = CypherContext.class)
@WebAppConfiguration
@SpringBootTest
public class CypherRestTest {

    private Logger logger = Logger.getLogger(this.getClass().getName());

    @Autowired
    private WebApplicationContext webCtx;

    @Autowired
    private ApplicationController applicationController;

    private MockMvc mockMvc;

    @Before
    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webCtx).build();

        // reset graph's id counter
        Graph graph = (Graph) ReflectionTestUtils.getField(applicationController, "graph");
        IdGenerator idGenerator = (IdGenerator) ReflectionTestUtils.getField(graph, "idGenerator");
        ReflectionTestUtils.setField(idGenerator, "lastGeneratedIndex", 0);

        // clear files with data and metadata
        FileUtils.clearFile("nodes.bin");
        FileUtils.clearFile("nodesMetaData.bin");
        FileUtils.clearFile("relationshipMetaData.bin");
        FileUtils.clearFile("relationship.bin");
        FileUtils.clearFile("graph.bin");
    }

    @Test
    public void createSingleNodeTest() throws Exception {
        // given
        ResultRowDto expectedResultRow = ResultRowDto.builder()
                .contentType(ContentType.NODE)
                .node(TestSpecification.expectedNode1)
                .build();

        Map.Entry<Integer, ResultRowDto> expectedEntryRow = new AbstractMap.SimpleEntry<>(0, expectedResultRow);

        // when
        MockHttpServletResponse response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command("CREATE (n: Person)"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        String RSAsString = response.getContentAsString();
        ResultDto result = TestUtils.castToResult(RSAsString);
        logger.info("RS = " + RSAsString);

        //then
        Assertions.assertThat(result.getCommand()).isEqualTo("CREATE (n: Person)");
        Assertions.assertThat(result.getContent()).hasSize(1);
        Assertions.assertThat(Matchers.sameAs(result.getContent().get(0).getNode(), TestSpecification.expectedNode1));
    }

    @Test
    public void createThreeNodeTest() throws Exception {
        // given
        ResultRowDto expectedResultRow1 = ResultRowDto.builder()
                .contentType(ContentType.NODE)
                .node(TestSpecification.expectedNode1)
                .build();

        ResultRowDto expectedResultRow2 = ResultRowDto.builder()
                .contentType(ContentType.NODE)
                .node(TestSpecification.expectedNode2)
                .build();

        ResultRowDto expectedResultRow3 = ResultRowDto.builder()
                .contentType(ContentType.NODE)
                .node(TestSpecification.expectedNode3)
                .build();

        List<ResultRowDto> expectedRowList = Arrays.asList(expectedResultRow1, expectedResultRow2, expectedResultRow3);


        // when
        for (int i=0; i<3; i++) {
            MockHttpServletResponse response = mockMvc.perform(post("/Cypher")
                    .contentType(MediaType.APPLICATION_JSON_UTF8)
                    .content(TestUtils.convertObjectToJsonText(new Command("CREATE (n: Person)"))))
                    .andExpect(status().isOk())
                    .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                    .andReturn().getResponse();
            String RSAsString = response.getContentAsString();
            logger.info("RS = " + RSAsString);
            ResultDto result = TestUtils.castToResult(RSAsString);

            //then
            Assertions.assertThat(result.getCommand()).isEqualTo("CREATE (n: Person)");
            Assertions.assertThat(result.getContent()).hasSize(1);
            Assertions.assertThat(Matchers.sameAs(result.getContent().get(0), expectedRowList.get(i)));
        }
    }

    @Test
    public void createAndDeleteNode() throws Exception {
        // given
        ResultRowDto expectedResultRow = ResultRowDto.builder()
                .contentType(ContentType.STRING)
                .message("Nodes deleted: 1")
                .build();

        MockHttpServletResponse response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command("CREATE (n: Person)"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        String RSAsString = response.getContentAsString();
        ResultDto result = TestUtils.castToResult(RSAsString);
        logger.info("RS = " + RSAsString);

        Assertions.assertThat(result.getCommand()).isEqualTo("CREATE (n: Person)");
        Assertions.assertThat(result.getContent()).hasSize(1);
        Assertions.assertThat(Matchers.sameAs(result.getContent().get(0).getNode(), TestSpecification.expectedNode1));

        // when
        response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command("MATCH (n: Person) DELETE n"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        RSAsString = response.getContentAsString();
        logger.info("RS = " + RSAsString);
        result = TestUtils.castToResult(RSAsString);

        // then
        Assertions.assertThat(result.getCommand()).isEqualTo("MATCH (n: Person) DELETE n");
        Assertions.assertThat(result.getContent()).hasSize(1);
        Assertions.assertThat(Matchers.sameAs(result.getContent().get(0), expectedResultRow));
    }

    @Test
    public void createAndUpdateNode() throws Exception {
        // given
        ResultRowDto expectedResultRow = ResultRowDto.builder()
                .contentType(ContentType.STRING)
                .message("Properties set: 1")
                .build();

        MockHttpServletResponse response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command("CREATE (n: Person)"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        String RSAsString = response.getContentAsString();
        ResultDto result = TestUtils.castToResult(RSAsString);
        logger.info("RS = " + RSAsString);

        Assertions.assertThat(result.getCommand()).isEqualTo("CREATE (n: Person)");
        Assertions.assertThat(result.getContent()).hasSize(1);
        Assertions.assertThat(Matchers.sameAs(result.getContent().get(0).getNode(), TestSpecification.expectedNode1));

        // when
        response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command("MATCH (n: Person) SET n.name = 'Kate'"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        RSAsString = response.getContentAsString();
        logger.info("RS = " + RSAsString);
        result = TestUtils.castToResult(RSAsString);

        // then
        Assertions.assertThat(result.getCommand()).isEqualTo("MATCH (n: Person) SET n.name = 'Kate'");
        Assertions.assertThat(result.getContent()).hasSize(1);
        Assertions.assertThat(Matchers.sameAs(result.getContent().get(0), expectedResultRow));
    }

    @Test
    public void createNodeAndRemovePropertyTest() throws Exception {
        // given
        ResultRowDto expectedResultRow = ResultRowDto.builder()
                .contentType(ContentType.STRING)
                .message("Properties removed: 1")
                .build();

        MockHttpServletResponse response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command("CREATE (n: Person)"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        String RSAsString = response.getContentAsString();
        ResultDto result = TestUtils.castToResult(RSAsString);
        logger.info("RS = " + RSAsString);

        Assertions.assertThat(result.getCommand()).isEqualTo("CREATE (n: Person)");
        Assertions.assertThat(result.getContent()).hasSize(1);
        Assertions.assertThat(Matchers.sameAs(result.getContent().get(0).getNode(), TestSpecification.expectedNode1));

        // when
        response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command("MATCH (n: Person) REMOVE n.name"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        RSAsString = response.getContentAsString();
        logger.info("RS = " + RSAsString);
        result = TestUtils.castToResult(RSAsString);

        // then
        Assertions.assertThat(result.getCommand()).isEqualTo("MATCH (n: Person) REMOVE n.name");
        Assertions.assertThat(result.getContent()).hasSize(1);
        Assertions.assertThat(Matchers.sameAs(result.getContent().get(0), expectedResultRow));
    }

    @Test
    public void createTwoNodesAndConnectThemByRelationshipTest() {

    }

    @Test
    public void createAndDeleteRelationshipTest() {

    }

    @Test
    public void createThreeNodesAndMatchNodesByPropertyTest() {

    }

    @Test
    public void Test() {

    }
}
