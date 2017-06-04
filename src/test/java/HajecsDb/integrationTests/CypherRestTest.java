package HajecsDb.integrationTests;

import HajecsDb.unitTests.utils.FileUtils;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.restLayer.ApplicationController;
import org.hajecsdb.graphs.restLayer.dto.Command;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;
import org.hajecsdb.graphs.restLayer.dto.ResultRowDto;
import org.hajecsdb.graphs.restLayer.dto.SessionDto;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;
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

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import static org.fest.assertions.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
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
        TransactionalGraphService graph = (TransactionalGraphService) ReflectionTestUtils.getField(applicationController, "transactionalGraphService");
//        IdGenerator tGraph = (IdGenerator) ReflectionTestUtils.getField(graph, "idGenerator");
//        IdGenerator idGenerator = (IdGenerator) ReflectionTestUtils.getField(graph, "idGenerator");
//        ReflectionTestUtils.setField(idGenerator, "lastGeneratedIndex", 0);

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
        SessionDto session = createSession();
        beginTransaction(session);

        // when
        MockHttpServletResponse response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command(session.getSessionId(), "CREATE (n: Person)"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        String RSAsString = response.getContentAsString();
        ResultDto result = TestUtils.castToResult(RSAsString);
        logger.info("RS = " + RSAsString);

        commitTransaction(session);
        String sessionStatus = closeSession(session);
        assertThat(sessionStatus).isEqualTo("Session closed!");

        //then
        assertThat(result.getCommand()).isEqualTo("CREATE (n: Person)");
        assertThat(result.getContent()).hasSize(1);
        assertThat(Matchers.sameAs(result.getContent().get(0).getNode(), TestSpecification.expectedNode1));
    }

    @Test
    public void createThreeNodesTest() throws Exception {
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
        SessionDto session = createSession();
        beginTransaction(session);

        for (int i = 0; i < 3; i++) {
            MockHttpServletResponse response = mockMvc.perform(post("/Cypher")
                    .contentType(MediaType.APPLICATION_JSON_UTF8)
                    .content(TestUtils.convertObjectToJsonText(new Command(session.getSessionId(), "CREATE (n: Person)"))))
                    .andExpect(status().isOk())
                    .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                    .andReturn().getResponse();
            String RSAsString = response.getContentAsString();
            logger.info("RS = " + RSAsString);
            ResultDto result = TestUtils.castToResult(RSAsString);

            //then
            assertThat(result.getCommand()).isEqualTo("CREATE (n: Person)");
            assertThat(result.getContent()).hasSize(1);
            assertThat(Matchers.sameAs(result.getContent().get(0), expectedRowList.get(i)));
        }

        commitTransaction(session);
        String sessionStatus = closeSession(session);
        assertThat(sessionStatus).isEqualTo("Session closed!");
    }

    @Test
    public void createAndDeleteNode() throws Exception {
        // given
        ResultRowDto expectedResultRow = ResultRowDto.builder()
                .contentType(ContentType.STRING)
                .message("Nodes deleted: 1")
                .build();


        SessionDto session = createSession();
        beginTransaction(session);

        MockHttpServletResponse response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command(session.getSessionId(), "CREATE (n: Person)"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        String RSAsString = response.getContentAsString();
        ResultDto result = TestUtils.castToResult(RSAsString);
        logger.info("RS = " + RSAsString);

        commitTransaction(session);

        assertThat(result.getCommand()).isEqualTo("CREATE (n: Person)");
        assertThat(result.getContent()).hasSize(1);
        assertThat(Matchers.sameAs(result.getContent().get(0).getNode(), TestSpecification.expectedNode1));


        // in second transaction node will be deleted

        // when
        beginTransaction(session);

        response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command(session.getSessionId(), "MATCH (n: Person) DELETE n"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        RSAsString = response.getContentAsString();
        logger.info("RS = " + RSAsString);
        result = TestUtils.castToResult(RSAsString);

        commitTransaction(session);
        String sessionStatus = closeSession(session);
        assertThat(sessionStatus).isEqualTo("Session closed!");

        // then
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Person) DELETE n");
        assertThat(result.getContent()).hasSize(1);
        assertThat(Matchers.sameAs(result.getContent().get(0), expectedResultRow));
    }

    @Test
    public void createAndUpdateNode() throws Exception {
        /*
        in first transaction will be created node
        in second transaction node will be updated
         */


        // given
        ResultRowDto expectedResultRow = ResultRowDto.builder()
                .contentType(ContentType.STRING)
                .message("Properties set: 1")
                .build();

        // when
        SessionDto session = createSession();
        beginTransaction(session);

        MockHttpServletResponse response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command(session.getSessionId(), "CREATE (n: Person)"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        String RSAsString = response.getContentAsString();
        ResultDto result = TestUtils.castToResult(RSAsString);
        logger.info("RS = " + RSAsString);
        commitTransaction(session);

        assertThat(result.getCommand()).isEqualTo("CREATE (n: Person)");
        assertThat(result.getContent()).hasSize(1);
        assertThat(Matchers.sameAs(result.getContent().get(0).getNode(), TestSpecification.expectedNode1));

        // when
        beginTransaction(session);
        response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command(session.getSessionId(), "MATCH (n: Person) SET n.name = 'Kate'"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        RSAsString = response.getContentAsString();
        logger.info("RS = " + RSAsString);
        result = TestUtils.castToResult(RSAsString);

        commitTransaction(session);
        String sessionStatus = closeSession(session);
        assertThat(sessionStatus).isEqualTo("Session closed!");

        // then
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Person) SET n.name = 'Kate'");
        assertThat(result.getContent()).hasSize(1);
        assertThat(Matchers.sameAs(result.getContent().get(0), expectedResultRow));
    }

    @Test
    public void createNodeAndRemovePropertyTest() throws Exception {
        /*
        in first transaction will be created node
        in second transaction property of node will be removed
         */

        // given
        ResultRowDto expectedResultRow = ResultRowDto.builder()
                .contentType(ContentType.STRING)
                .message("Properties removed: 1")
                .build();


        SessionDto session = createSession();
        beginTransaction(session);

        MockHttpServletResponse response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command(session.getSessionId(), "CREATE (n: Person)"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        String RSAsString = response.getContentAsString();
        ResultDto result = TestUtils.castToResult(RSAsString);
        logger.info("RS = " + RSAsString);

        commitTransaction(session);

        assertThat(result.getCommand()).isEqualTo("CREATE (n: Person)");
        assertThat(result.getContent()).hasSize(1);
        assertThat(Matchers.sameAs(result.getContent().get(0).getNode(), TestSpecification.expectedNode1));

        // when
        beginTransaction(session);
        response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command("sessionId", "MATCH (n: Person) REMOVE n.name"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        RSAsString = response.getContentAsString();
        logger.info("RS = " + RSAsString);
        result = TestUtils.castToResult(RSAsString);

        commitTransaction(session);

        String sessionStatus = closeSession(session);
        assertThat(sessionStatus).isEqualTo("Session closed!");

        // then
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Person) REMOVE n.name");
        assertThat(result.getContent()).hasSize(1);
        assertThat(Matchers.sameAs(result.getContent().get(0), expectedResultRow));
    }

    @Test
    public void createTwoNodesAndConnectThemByRelationshipTest() throws Exception {

        // given
        ResultRowDto expectedResultRow1 = ResultRowDto.builder()
                .contentType(ContentType.NODE)
                .node(TestSpecification.expectedNode1)
                .build();

        ResultRowDto expectedResultRow2 = ResultRowDto.builder()
                .contentType(ContentType.NODE)
                .node(TestSpecification.expectedNode2)
                .build();

        List<ResultRowDto> expectedRowList = Arrays.asList(expectedResultRow1, expectedResultRow2);


        // TRANSACTION 1

        // when
        SessionDto session = createSession();
        beginTransaction(session);

        MockHttpServletResponse response1 = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command(session.getSessionId(), "CREATE (n: User)"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();

        MockHttpServletResponse response2 = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command(session.getSessionId(), "CREATE (n: Student)"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();

        commitTransaction(session);

        // TRANSACTION 2
        beginTransaction(session);

        StringBuilder commandBuilder = new StringBuilder()
                .append("MATCH (u: User) ")
                .append("MATCH (r: Student) ")
                .append("CREATE (u)-[p:HAS_ROLE]->(r)");

        MockHttpServletResponse response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command(session.getSessionId(), commandBuilder.toString()))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        String RSAsString = response.getContentAsString();
        logger.info("RS = " + RSAsString);
        ResultDto result = TestUtils.castToResult(RSAsString);

        //then
        assertThat(result.getCommand()).isEqualTo(commandBuilder.toString());
        assertThat(result.getContent()).hasSize(1);
//        assertThat(Matchers.sameAs(result.getContent().get(0), expectedRowList.get(0)));

        commitTransaction(session);
        String sessionStatus = closeSession(session);
        assertThat(sessionStatus).isEqualTo("Session closed!");

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

    private SessionDto createSession() throws Exception {
        MockHttpServletResponse response = mockMvc.perform(get("/Session")
                .contentType(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        String RSAsString = response.getContentAsString();
        logger.info("RS = " + RSAsString);
        return TestUtils.castToSession(RSAsString);
    }

    private ResultDto beginTransaction(SessionDto session) throws Exception {
        MockHttpServletResponse response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command(session.getSessionId(), "BEGIN"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        String RSAsString = response.getContentAsString();
        logger.info("RS = " + RSAsString);
        return TestUtils.castToResult(RSAsString);
    }

    private ResultDto commitTransaction(SessionDto session) throws Exception {
        MockHttpServletResponse response = mockMvc.perform(post("/Cypher")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(new Command(session.getSessionId(), "COMMIT"))))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn().getResponse();
        String RSAsString = response.getContentAsString();
        logger.info("RS = " + RSAsString);
        return TestUtils.castToResult(RSAsString);
    }

    private String closeSession(SessionDto session) throws Exception {
        MockHttpServletResponse response = mockMvc.perform(delete("/Session")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.convertObjectToJsonText(session)))
                .andExpect(status().isOk())
                .andReturn().getResponse();
        String RSAsString = response.getContentAsString();
        logger.info("RS = " + RSAsString);
        return RSAsString;
    }
}
