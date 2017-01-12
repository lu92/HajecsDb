package HajecsDb;

import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.storage.ByteHelper;
import org.hajecsdb.graphs.storage.entities.BinaryProperties;
import org.hajecsdb.graphs.storage.entities.BinaryProperty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.*;

@RunWith(MockitoJUnitRunner.class)
public class BinaryPropertiesTest {

    private ByteHelper byteHelper = new ByteHelper();

    @Test
    public void serializeEmptyPropertiesTest() {
        // given
        Properties properties = new Properties();

        // when
        BinaryProperties binaryProperties = byteHelper.convertPropertiesIntoBinaryFigure(properties);

        // then
        int numberOfProperties = ByteBuffer.wrap(Arrays.copyOfRange(binaryProperties.getBytes(), 0, Integer.BYTES)).getInt();
        assertThat(numberOfProperties).isEqualTo(0);
        assertThat(binaryProperties.getNumberOfProperties()).isEqualTo(0);
        assertThat(binaryProperties.getBinaryProperties()).isEmpty();
        assertThat(binaryProperties.getBytes().length).isEqualTo(Integer.BYTES);
    }

    @Test
    public void serializePropertiesWithOnePropertyTest() {
        // given
        Property property = new Property("id", 1l, LONG);
        Properties properties = new Properties().add(property);
        BinaryProperty expectedBinaryProperty = byteHelper.convertPropertiesIntoBinaryFigure(property);

        // when
        BinaryProperties binaryProperties = byteHelper.convertPropertiesIntoBinaryFigure(properties);

        // then
        assertThat(binaryProperties.getNumberOfProperties()).isEqualTo(1);
        assertThat(binaryProperties.getBinaryProperties()).hasSize(1);
        assertThat(binaryProperties.getBinaryProperties()).containsExactly(expectedBinaryProperty);
    }

    @Test
    public void serializePropertiesTest() {
        // given
        Property firstNameProperty = new Property("firstName", "James", STRING);
        Property lastNameProperty = new Property("lastName", "Bond", STRING);
        Property ageProperty = new Property("age", 40, INT);

        Properties properties = new Properties()
                .add(firstNameProperty)
                .add(lastNameProperty)
                .add(ageProperty);

        BinaryProperty ExpectedBinaryFirstNameProperty = byteHelper.convertPropertiesIntoBinaryFigure(firstNameProperty);
        BinaryProperty ExpectedBinaryLastNameProperty = byteHelper.convertPropertiesIntoBinaryFigure(lastNameProperty);
        BinaryProperty ExpectedBinaryAgeProperty = byteHelper.convertPropertiesIntoBinaryFigure(ageProperty);

        // when
        BinaryProperties binaryProperties = byteHelper.convertPropertiesIntoBinaryFigure(properties);

        // then
        assertThat(binaryProperties.getNumberOfProperties()).isEqualTo(3);
        assertThat(binaryProperties.getBinaryProperties())
                .containsExactly(firstNameProperty, lastNameProperty, ageProperty);
    }

    @Test
    public void serializePropertiesAndDoShiftTest() {
        Properties properties = new Properties()
                .add("firstName", "James", STRING)
                .add("lastName", "Bond", STRING)
                .add("age", 40, INT);

        BinaryProperties binaryProperties = byteHelper.convertPropertiesIntoBinaryFigure(properties);
        binaryProperties.shiftIndexes(1024);

        assertThat(binaryProperties.getNumberOfProperties()).isEqualTo(3);
    }
}
