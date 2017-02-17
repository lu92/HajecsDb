package HajecsDb.graphSerialization;

import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.PropertyType;
import org.hajecsdb.graphs.storage.mappers.PropertiesBinaryMapper;
import org.hajecsdb.graphs.storage.entities.BinaryProperties;
import org.hajecsdb.graphs.storage.entities.BinaryProperty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.*;

@RunWith(MockitoJUnitRunner.class)
public class PropertiesBinaryMapperTest {

    private PropertiesBinaryMapper propertiesBinaryMapper = new PropertiesBinaryMapper();

    @Test
    public void intValueTest() {
        Property property = new Property("id", INT, 128);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getType()).isEqualTo(PropertyType.INT);
        assertThat(binaryProperty.getValue()).isEqualTo(new Integer(128));

        Property mappedProperty = propertiesBinaryMapper.toProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getType()).isEqualTo(PropertyType.INT);
        assertThat(mappedProperty.getValue()).isEqualTo(new Integer(128));
    }

    @Test
    public void fromBinaryFigureToIntValueTest() {
        // given
        Property expectedProperty = new Property("id", INT, 128);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(expectedProperty);

        // when
        Property property = propertiesBinaryMapper.fromBinaryFigure(binaryProperty.getBytes());

        // then
        assertThat(property.getKey()).isEqualTo("id");
        assertThat(property.getType()).isEqualTo(PropertyType.INT);
        assertThat(property.getValue()).isEqualTo(new Integer(128));
    }

    @Test
    public void longValueTest() {
        Property property = new Property("id", LONG, 128l);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getType()).isEqualTo(PropertyType.LONG);
        assertThat(binaryProperty.getValue()).isEqualTo(new Long(128));

        Property mappedProperty = propertiesBinaryMapper.toProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getType()).isEqualTo(PropertyType.LONG);
        assertThat(mappedProperty.getValue()).isEqualTo(new Long(128));
    }

    @Test
    public void fromBinaryFigureToLongValueTest() {
        // given
        Property expectedProperty = new Property("id", LONG, 128l);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(expectedProperty);

        // when
        Property property = propertiesBinaryMapper.fromBinaryFigure(binaryProperty.getBytes());

        //then
        assertThat(property.getKey()).isEqualTo("id");
        assertThat(property.getType()).isEqualTo(PropertyType.LONG);
        assertThat(property.getValue()).isEqualTo(new Long(128));
    }

    @Test
    public void floatValueTest() {
        Property property = new Property("id", FLOAT, 128.0f);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getType()).isEqualTo(FLOAT);
        assertThat(binaryProperty.getValue()).isEqualTo(new Float(128.0f));

        Property mappedProperty = propertiesBinaryMapper.toProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getType()).isEqualTo(FLOAT);
        assertThat(mappedProperty.getValue()).isEqualTo(new Float(128.0f));
    }

    @Test
    public void fromBinaryFigureToFloatValueTest() {
        // given
        Property expectedProperty = new Property("id", FLOAT, 128.0f);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(expectedProperty);

        // when
        Property property = propertiesBinaryMapper.fromBinaryFigure(binaryProperty.getBytes());

        //then
        assertThat(property.getKey()).isEqualTo("id");
        assertThat(property.getType()).isEqualTo(FLOAT);
        assertThat(property.getValue()).isEqualTo(new Float(128.0f));
    }

    @Test
    public void doubleValueTest() {
        Property property = new Property("id", DOUBLE, 128.00);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getType()).isEqualTo(PropertyType.DOUBLE);
        assertThat(binaryProperty.getValue()).isEqualTo(new Double(128.00));

        Property mappedProperty = propertiesBinaryMapper.toProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getType()).isEqualTo(PropertyType.DOUBLE);
        assertThat(mappedProperty.getValue()).isEqualTo(new Double(128));
    }

    @Test
    public void fromBinaryFigureToDoubleValueTest() {
        // given
        Property expectedProperty = new Property("id", DOUBLE, 128.00);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(expectedProperty);

        // when
        Property property = propertiesBinaryMapper.fromBinaryFigure(binaryProperty.getBytes());

        //then
        assertThat(property.getKey()).isEqualTo("id");
        assertThat(property.getType()).isEqualTo(PropertyType.DOUBLE);
        assertThat(property.getValue()).isEqualTo(new Double(128));
    }

    @Test
    public void stringValueTest() {
        Property property = new Property("id", STRING, "One hundred twenty eight");
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getType()).isEqualTo(PropertyType.STRING);
        assertThat(binaryProperty.getValue()).isEqualTo("One hundred twenty eight");

        Property mappedProperty = propertiesBinaryMapper.toProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getType()).isEqualTo(PropertyType.STRING);
        assertThat(mappedProperty.getValue()).isEqualTo("One hundred twenty eight");
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
        // given
        Property property = new Property("id", LONG, 1l);
        Properties properties = new Properties().add(property);
        BinaryProperty expectedBinaryProperty = propertiesBinaryMapper.toBinaryFigure(property);

        // when
        BinaryProperties binaryProperties = propertiesBinaryMapper.toBinaryFigure(properties);

        // then
        assertThat(binaryProperties.getNumberOfProperties()).isEqualTo(1);
        List<BinaryProperty> mappedBinaryProperties = binaryProperties.getBinaryProperties();
        assertThat(mappedBinaryProperties).hasSize(1);
        assertThat(mappedBinaryProperties).containsExactly(expectedBinaryProperty);
        assertThat(mappedBinaryProperties.get(0).getKey()).isEqualTo("id");
        assertThat(mappedBinaryProperties.get(0).getType()).isEqualTo(LONG);
        assertThat(mappedBinaryProperties.get(0).getValue()).isEqualTo(1l);

        // validate numberOfProperties
        int numberOfProperties = ByteBuffer.wrap(Arrays.copyOfRange(binaryProperties.getBytes(), 0, Integer.BYTES)).getInt();
        assertThat(numberOfProperties).isEqualTo(1);

        // validate beginIndex
        long beginIndex = ByteBuffer.wrap(Arrays.copyOfRange(binaryProperties.getBytes(), Integer.BYTES, Integer.BYTES + Long.BYTES)).getLong();
        assertThat(beginIndex).isEqualTo(0l);

        // validate lastIndex
        long lastIndex = ByteBuffer.wrap(Arrays.copyOfRange(binaryProperties.getBytes(), Integer.BYTES + Long.BYTES, Integer.BYTES + Long.BYTES + Long.BYTES)).getLong();
        assertThat(lastIndex).isEqualTo(Integer.BYTES + 4 * Long.BYTES + expectedBinaryProperty.getLength());

        // validate beginBinaryPropertySection
        long beginBinaryPropertySection = ByteBuffer.wrap(Arrays.copyOfRange(binaryProperties.getBytes(), Integer.BYTES + 2 * Long.BYTES, Integer.BYTES + 3 * Long.BYTES)).getLong();
        assertThat(beginBinaryPropertySection).isEqualTo(Integer.BYTES + 4 * Long.BYTES);

        // validate endBinaryPropertySection
        long endBinaryPropertySection = ByteBuffer.wrap(Arrays.copyOfRange(binaryProperties.getBytes(), Integer.BYTES + 3 * Long.BYTES, Integer.BYTES + 4 * Long.BYTES)).getLong();
        assertThat(endBinaryPropertySection).isEqualTo(Integer.BYTES + 4 * Long.BYTES + expectedBinaryProperty.getLength());


        // validate savedProperty
        byte[] propertySection = Arrays.copyOfRange(binaryProperties.getBytes(), (int) beginBinaryPropertySection, (int) endBinaryPropertySection);
        Property retrievedProperty = propertiesBinaryMapper.toProperty(propertySection);
        assertThat(retrievedProperty.getKey()).isEqualTo("id");
        assertThat(retrievedProperty.getType()).isEqualTo(LONG);
        assertThat(retrievedProperty.getValue()).isEqualTo(1l);
    }

    @Test
    public void convertPropertiesWithThreePropertiesTest() {
        // given
        Property firstNameProperty = new Property("firstName", STRING, "James");
        Property lastNameProperty = new Property("lastName", STRING, "Bond");
        Property ageProperty = new Property("age", INT, 40);

        Properties properties = new Properties()
                .add(firstNameProperty)
                .add(lastNameProperty)
                .add(ageProperty);

        BinaryProperty expectedBinaryFirstNameProperty = propertiesBinaryMapper.toBinaryFigure(firstNameProperty);
        BinaryProperty expectedBinaryLastNameProperty = propertiesBinaryMapper.toBinaryFigure(lastNameProperty);
        BinaryProperty expectedBinaryAgeProperty = propertiesBinaryMapper.toBinaryFigure(ageProperty);

        // when
        BinaryProperties binaryProperties = propertiesBinaryMapper.toBinaryFigure(properties);

        // then
        assertThat(binaryProperties.getNumberOfProperties()).isEqualTo(3);
        List<BinaryProperty> binaryPropertyList = binaryProperties.getBinaryProperties();
        assertThat(binaryPropertyList).hasSize(3);
        assertThat(binaryPropertyList)
                .contains(expectedBinaryFirstNameProperty, expectedBinaryLastNameProperty, expectedBinaryAgeProperty);

        // validate numberOfProperties
        int numberOfProperties = ByteBuffer.wrap(Arrays.copyOfRange(binaryProperties.getBytes(), 0, Integer.BYTES)).getInt();
        assertThat(numberOfProperties).isEqualTo(3);

        // validate beginIndex
        long beginIndex = ByteBuffer.wrap(Arrays.copyOfRange(binaryProperties.getBytes(), Integer.BYTES, Integer.BYTES + Long.BYTES)).getLong();
        assertThat(beginIndex).isEqualTo(0l);

        // validate lastIndex
        long lastIndex = ByteBuffer.wrap(Arrays.copyOfRange(binaryProperties.getBytes(), Integer.BYTES + Long.BYTES, Integer.BYTES + Long.BYTES + Long.BYTES)).getLong();
        assertThat(lastIndex).isEqualTo(Integer.BYTES + 8*Long.BYTES +
                expectedBinaryFirstNameProperty.getLength() + expectedBinaryLastNameProperty.getLength() + expectedBinaryAgeProperty.getLength());
    }

    @Test
    public void convertBytesWithSinglePropertyToBinaryPropertiesTest() {
        // given
        Property property = new Property("id", LONG, 1l);
        BinaryProperty expectedBinaryProperty = propertiesBinaryMapper.toBinaryFigure(property);
        BinaryProperties binaryPropertiesSample = new BinaryProperties();
        binaryPropertiesSample.addProperty(expectedBinaryProperty);

        // when
        BinaryProperties binaryProperties = propertiesBinaryMapper.fromBinaryFigureToBinaryProperties(binaryPropertiesSample.getBytes());

        // then
        List<BinaryProperty> binaryPropertyList = binaryProperties.getBinaryProperties();
        assertThat(binaryPropertyList).hasSize(1);
        assertThat(binaryPropertyList).containsExactly(expectedBinaryProperty);
        assertThat(binaryPropertyList.get(0).getKey()).isEqualTo("id");
        assertThat(binaryPropertyList.get(0).getType()).isEqualTo(LONG);
        assertThat(binaryPropertyList.get(0).getValue()).isEqualTo(1l);
    }

    @Test
    public void convertBytesWithTriplePropertyToBinaryPropertiesTest() {
        // given
        Property firstNameProperty = new Property("firstName", STRING, "James");
        Property lastNameProperty = new Property("lastName", STRING, "Bond");
        Property ageProperty = new Property("age", INT, 40);
        BinaryProperty expectedBinaryProperty1 = propertiesBinaryMapper.toBinaryFigure(firstNameProperty);
        BinaryProperty expectedBinaryProperty2 = propertiesBinaryMapper.toBinaryFigure(lastNameProperty);
        BinaryProperty expectedBinaryProperty3 = propertiesBinaryMapper.toBinaryFigure(ageProperty);
        BinaryProperties binaryPropertiesSample = new BinaryProperties();
        binaryPropertiesSample.addProperty(expectedBinaryProperty1);
        binaryPropertiesSample.addProperty(expectedBinaryProperty2);
        binaryPropertiesSample.addProperty(expectedBinaryProperty3);

        // when
        BinaryProperties binaryProperties = propertiesBinaryMapper.fromBinaryFigureToBinaryProperties(binaryPropertiesSample.getBytes());

        // then
        List<BinaryProperty> binaryPropertyList = binaryProperties.getBinaryProperties();
        assertThat(binaryPropertyList).hasSize(3);
        assertThat(binaryPropertyList).containsExactly(expectedBinaryProperty1, expectedBinaryProperty2, expectedBinaryProperty3);

        assertThat(binaryPropertyList.get(0).getKey()).isEqualTo("firstName");
        assertThat(binaryPropertyList.get(0).getType()).isEqualTo(STRING);
        assertThat(binaryPropertyList.get(0).getValue()).isEqualTo("James");

        assertThat(binaryPropertyList.get(1).getKey()).isEqualTo("lastName");
        assertThat(binaryPropertyList.get(1).getType()).isEqualTo(STRING);
        assertThat(binaryPropertyList.get(1).getValue()).isEqualTo("Bond");

        assertThat(binaryPropertyList.get(2).getKey()).isEqualTo("age");
        assertThat(binaryPropertyList.get(2).getType()).isEqualTo(INT);
        assertThat(binaryPropertyList.get(2).getValue()).isEqualTo(40);
    }
}
