package com.krux.mm.tests;


import org.junit.Test;
import java.util.Properties;


import static junit.framework.TestCase.assertEquals;


public class PropsCloneTest {

    @Test
    public void test() {
        Properties props = new Properties();
        props.setProperty( "cass", "was here" );

        Properties propsClone = (Properties) props.clone();

        propsClone.setProperty( "cass", "wasn't here" );

        assertEquals( props.getProperty( "cass" ), "was here" );
        assertEquals( propsClone.getProperty( "cass" ), "wasn't here" );

    }

}
