package HajecsDb;

import org.hajecsdb.graphs.storage.serializers.PropertiesBinaryMapper;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class BinaryPropertiesTest {

    private PropertiesBinaryMapper propertiesBinaryMapper = new PropertiesBinaryMapper();

//    @Test
//    public void convertEmptyPropertiesTest() {
//        // given
//        Properties properties = new Properties();
//
//        // when
//        BinaryProperties binaryProperties = propertiesBinaryMapper.toBinaryFigure(properties);
//
//        // then
//        int numberOfProperties = ByteBuffer.wrap(Arrays.copyOfRange(binaryProperties.getBytes(), 0, Integer.BYTES)).getInt();
//        assertThat(numberOfProperties).isEqualTo(0);
//        assertThat(binaryProperties.getNumberOfProperties()).isEqualTo(0);
//        assertThat(binaryProperties.getBinaryProperties()).isEmpty();
//        assertThat(binaryProperties.getBytes().length).isEqualTo(Integer.BYTES);
//    }
//
//    @Test
//    public void convertPropertiesWithOnePropertyTest() {
//        // given
//        Property property = new Property("id", 1l, LONG);
//        Properties properties = new Properties().add(property);
//        BinaryProperty expectedBinaryProperty = propertiesBinaryMapper.toBinaryFigure(property);
//
//        // when
//        BinaryProperties binaryProperties = propertiesBinaryMapper.toBinaryFigure(properties);
//
//        // then
//        assertThat(binaryProperties.getNumberOfProperties()).isEqualTo(1);
//        assertThat(binaryProperties.getBinaryProperties()).hasSize(1);
//        assertThat(binaryProperties.getBinaryProperties()).containsExactly(expectedBinaryProperty);
//    }
//
//    @Test
//    public void convertPropertiesWithThreePropertiesTest() {
//        // given
//        Property firstNameProperty = new Property("firstName", "James", STRING);
//        Property lastNameProperty = new Property("lastName", "Bond", STRING);
//        Property ageProperty = new Property("age", 40, INT);
//
//        Properties properties = new Properties()
//                .add(firstNameProperty)
//                .add(lastNameProperty)
//                .add(ageProperty);
//
//        BinaryProperty ExpectedBinaryFirstNameProperty = propertiesBinaryMapper.toBinaryFigure(firstNameProperty);
//        BinaryProperty ExpectedBinaryLastNameProperty = propertiesBinaryMapper.toBinaryFigure(lastNameProperty);
//        BinaryProperty ExpectedBinaryAgeProperty = propertiesBinaryMapper.toBinaryFigure(ageProperty);
//
//        // when
//        BinaryProperties binaryProperties = propertiesBinaryMapper.toBinaryFigure(properties);
//
//        // then
//        assertThat(binaryProperties.getNumberOfProperties()).isEqualTo(3);
//        assertThat(binaryProperties.getBinaryProperties()).hasSize(3);
//        assertThat(binaryProperties.getBinaryProperties())
//                .contains(ExpectedBinaryFirstNameProperty, ExpectedBinaryLastNameProperty, ExpectedBinaryAgeProperty);
//    }
}
