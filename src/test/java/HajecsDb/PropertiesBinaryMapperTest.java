package HajecsDb;

import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.PropertyType;
import org.hajecsdb.graphs.storage.serializers.PropertiesBinaryMapper;
import org.hajecsdb.graphs.storage.entities.BinaryProperties;
import org.hajecsdb.graphs.storage.entities.BinaryProperty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.INT;
import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class PropertiesBinaryMapperTest {

    private PropertiesBinaryMapper propertiesBinaryMapper = new PropertiesBinaryMapper();

    @Test
    public void intValueTest() {
        Property property = new Property("id", 128, PropertyType.INT);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getValue()).isEqualTo(new Integer(128));
        assertThat(binaryProperty.getType()).isEqualTo(PropertyType.INT);

        Property mappedProperty = propertiesBinaryMapper.toProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getValue()).isEqualTo(new Integer(128));
        assertThat(mappedProperty.getType()).isEqualTo(PropertyType.INT);
    }

    @Test
    public void longValueTest() {
        Property property = new Property("id", 128l, PropertyType.LONG);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getValue()).isEqualTo(new Long(128));
        assertThat(binaryProperty.getType()).isEqualTo(PropertyType.LONG);

        Property mappedProperty = propertiesBinaryMapper.toProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getValue()).isEqualTo(new Long(128));
        assertThat(mappedProperty.getType()).isEqualTo(PropertyType.LONG);
    }

    @Test
    public void floatValueTest() {
        Property property = new Property("id", 128.0f, PropertyType.FLOAT);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getValue()).isEqualTo(new Float(128.0f));
        assertThat(binaryProperty.getType()).isEqualTo(PropertyType.FLOAT);

        Property mappedProperty = propertiesBinaryMapper.toProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getValue()).isEqualTo(new Float(128.0f));
        assertThat(mappedProperty.getType()).isEqualTo(PropertyType.FLOAT);
    }

    @Test
    public void doubleValueTest() {
        Property property = new Property("id", 128.00, PropertyType.DOUBLE);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getValue()).isEqualTo(new Double(128.00));
        assertThat(binaryProperty.getType()).isEqualTo(PropertyType.DOUBLE);

        Property mappedProperty = propertiesBinaryMapper.toProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getValue()).isEqualTo(new Double(128));
        assertThat(mappedProperty.getType()).isEqualTo(PropertyType.DOUBLE);
    }

    @Test
    public void stringValueTest() {
        Property property = new Property("id", "One hundred twenty eight", PropertyType.STRING);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getValue()).isEqualTo("One hundred twenty eight");
        assertThat(binaryProperty.getType()).isEqualTo(PropertyType.STRING);

        Property mappedProperty = propertiesBinaryMapper.toProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getValue()).isEqualTo("One hundred twenty eight");
        assertThat(mappedProperty.getType()).isEqualTo(PropertyType.STRING);
    }

    @Test
    public void convertEmptyPropertiesTest() {
        // given
        Properties properties = new Properties();

        // when
        BinaryProperties binaryProperties = propertiesBinaryMapper.toBinaryFigure(properties);

        // then
        assertThat(binaryProperties.getNumberOfProperties()).isEqualTo(0);
        assertThat(binaryProperties.getBinaryProperties()).isEmpty();
        int numberOfProperties = ByteBuffer.wrap(Arrays.copyOfRange(binaryProperties.getBytes(), 0, Integer.BYTES)).getInt();
        assertThat(numberOfProperties).isEqualTo(0);
        assertThat(binaryProperties.getBytes().length).isEqualTo(Integer.BYTES);


    }

    @Test
    public void convertPropertiesWithOnePropertyTest() {
        Property property = new Property("id", 1l, LONG);
        Properties properties = new Properties().add(property);
        BinaryProperty expectedBinaryProperty = propertiesBinaryMapper.toBinaryFigure(property);

        BinaryProperties binaryProperties = propertiesBinaryMapper.toBinaryFigure(properties);
        assertThat(binaryProperties.getNumberOfProperties()).isEqualTo(1);
        assertThat(binaryProperties.getBinaryProperties()).hasSize(1);
        assertThat(binaryProperties.getBinaryProperties()).containsExactly(expectedBinaryProperty);

//        propertiesBinaryMapper.convertBinaryFigureIntoProperties(binaryProperties);
        int numberOfProperties = ByteBuffer.wrap(Arrays.copyOfRange(binaryProperties.getBytes(), 0, Integer.BYTES)).getInt();
        assertThat(numberOfProperties).isEqualTo(1);

        long beginIndex = ByteBuffer.wrap(Arrays.copyOfRange(binaryProperties.getBytes(), Integer.BYTES, Integer.BYTES+Long.BYTES)).getLong();
        assertThat(beginIndex).isEqualTo(Integer.BYTES);
    }

    @Test
    public void convertPropertiesWithThreePropertiesTest() {
        // given
        Property firstNameProperty = new Property("firstName", "James", STRING);
        Property lastNameProperty = new Property("lastName", "Bond", STRING);
        Property ageProperty = new Property("age", 40, INT);

        Properties properties = new Properties()
                .add(firstNameProperty)
                .add(lastNameProperty)
                .add(ageProperty);

        BinaryProperty ExpectedBinaryFirstNameProperty = propertiesBinaryMapper.toBinaryFigure(firstNameProperty);
        BinaryProperty ExpectedBinaryLastNameProperty = propertiesBinaryMapper.toBinaryFigure(lastNameProperty);
        BinaryProperty ExpectedBinaryAgeProperty = propertiesBinaryMapper.toBinaryFigure(ageProperty);

        // when
        BinaryProperties binaryProperties = propertiesBinaryMapper.toBinaryFigure(properties);

        // then
        assertThat(binaryProperties.getNumberOfProperties()).isEqualTo(3);
        assertThat(binaryProperties.getBinaryProperties()).hasSize(3);
        assertThat(binaryProperties.getBinaryProperties())
                .contains(ExpectedBinaryFirstNameProperty, ExpectedBinaryLastNameProperty, ExpectedBinaryAgeProperty);
    }
}
