package com.krux.mm.tests;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PropsCloneTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

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
