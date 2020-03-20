/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.packstream;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.connector.internal.util.Iterables;
import org.neo4j.connector.packstream.PackStream;
import org.neo4j.connector.packstream.PackType;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PackStreamTest
{
    public static Map<String, Object> asMap( Object... keysAndValues )
    {
        Map<String,Object> map = Iterables.newLinkedHashMapWithSize( keysAndValues.length / 2 );
        String key = null;
        for ( Object keyOrValue : keysAndValues )
        {
            if ( key == null )
            {
                key = keyOrValue.toString();
            }
            else
            {
                map.put( key, keyOrValue );
                key = null;
            }
        }
        return map;
    }

    private static class Machine
    {

        private final ByteArrayOutputStream output;
        private final WritableByteChannel writable;
        private final PackStream.Packer packer;

        Machine()
        {
            this.output = new ByteArrayOutputStream();
            this.writable = Channels.newChannel( this.output );
            this.packer = new PackStream.Packer( new ChannelOutput( this.writable ) );
        }

        public void reset()
        {
            output.reset();
        }

        public byte[] output()
        {
            return output.toByteArray();
        }

        PackStream.Packer packer()
        {
            return packer;
        }
    }

    private PackStream.Unpacker newUnpacker( byte[] bytes )
    {
        ByteArrayInputStream input = new ByteArrayInputStream( bytes );
        return new PackStream.Unpacker( new BufferedChannelInput( Channels.newChannel( input ) ) );
    }

    @Test
    void testCanPackAndUnpackNull() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        machine.packer().packNull();

        // Then
        byte[] bytes = machine.output();
        MatcherAssert.assertThat( bytes, CoreMatchers.equalTo( new byte[]{(byte) 0xC0} ) );

        // When
        PackStream.Unpacker unpacker = newUnpacker( bytes );
        PackType packType = unpacker.peekNextType();

        // Then
        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.NULL ) );

    }

    @Test
    void testCanPackAndUnpackTrue() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        machine.packer().pack( true );

        // Then
        byte[] bytes = machine.output();
        MatcherAssert.assertThat( bytes, CoreMatchers.equalTo( new byte[]{(byte) 0xC3} ) );

        // When
        PackStream.Unpacker unpacker = newUnpacker( bytes );
        PackType packType = unpacker.peekNextType();

        // Then
        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.BOOLEAN ) );
        MatcherAssert.assertThat( unpacker.unpackBoolean(), CoreMatchers.equalTo( true ) );

    }

    @Test
    void testCanPackAndUnpackFalse() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        machine.packer().pack( false );

        // Then
        byte[] bytes = machine.output();
        MatcherAssert.assertThat( bytes, CoreMatchers.equalTo( new byte[]{(byte) 0xC2} ) );

        // When
        PackStream.Unpacker unpacker = newUnpacker( bytes );
        PackType packType = unpacker.peekNextType();

        // Then
        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.BOOLEAN ) );
        MatcherAssert.assertThat( unpacker.unpackBoolean(), CoreMatchers.equalTo( false ) );

    }

    @Test
    void testCanPackAndUnpackTinyIntegers() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        for ( long i = -16; i < 128; i++ )
        {
            // When
            machine.reset();
            machine.packer().pack( i );

            // Then
            byte[] bytes = machine.output();
            MatcherAssert.assertThat( bytes.length, CoreMatchers.equalTo( 1 ) );

            // When
            PackStream.Unpacker unpacker = newUnpacker( bytes );
            PackType packType = unpacker.peekNextType();

            // Then
            MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.INTEGER ) );
            MatcherAssert.assertThat( unpacker.unpackLong(), CoreMatchers.equalTo( i ) );
        }
    }

    @Test
    void testCanPackAndUnpackShortIntegers() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        for ( long i = -32768; i < 32768; i++ )
        {
            // When
            machine.reset();
            machine.packer().pack( i );

            // Then
            byte[] bytes = machine.output();
            MatcherAssert.assertThat( bytes.length, Matchers.lessThanOrEqualTo( 3 ) );

            // When
            PackStream.Unpacker unpacker = newUnpacker( bytes );
            PackType packType = unpacker.peekNextType();

            // Then
            MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.INTEGER ) );
            MatcherAssert.assertThat( unpacker.unpackLong(), CoreMatchers.equalTo( i ) );

        }

    }

    @Test
    void testCanPackAndUnpackPowersOfTwoAsIntegers() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        for ( int i = 0; i < 32; i++ )
        {
            long n = (long) Math.pow( 2, i );

            // When
            machine.reset();
            machine.packer().pack( n );

            // Then
            PackStream.Unpacker unpacker = newUnpacker( machine.output() );
            PackType packType = unpacker.peekNextType();

            // Then
            MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.INTEGER ) );
            MatcherAssert.assertThat( unpacker.unpackLong(), CoreMatchers.equalTo( n ) );

        }

    }

    @Test
    void testCanPackAndUnpackPowersOfTwoPlusABitAsDoubles() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        for ( int i = 0; i < 32; i++ )
        {
            double n = Math.pow( 2, i ) + 0.5;

            // When
            machine.reset();
            machine.packer().pack( n );

            // Then
            PackStream.Unpacker unpacker = newUnpacker( machine.output() );
            PackType packType = unpacker.peekNextType();

            // Then
            MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.FLOAT ) );
            MatcherAssert.assertThat( unpacker.unpackDouble(), CoreMatchers.equalTo( n ) );

        }

    }

    @Test
    void testCanPackAndUnpackPowersOfTwoMinusABitAsDoubles() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        for ( int i = 0; i < 32; i++ )
        {
            double n = Math.pow( 2, i ) - 0.5;

            // When
            machine.reset();
            machine.packer().pack( n );

            // Then
            PackStream.Unpacker unpacker = newUnpacker( machine.output() );
            PackType packType = unpacker.peekNextType();

            // Then
            MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.FLOAT ) );
            MatcherAssert.assertThat( unpacker.unpackDouble(), CoreMatchers.equalTo( n ) );

        }

    }

    @Test
    void testCanPackAndUnpackByteArrays() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        testByteArrayPackingAndUnpacking( machine, 0 );
        for ( int i = 0; i < 24; i++ )
        {
            testByteArrayPackingAndUnpacking( machine, (int) Math.pow( 2, i ) );
        }
    }

    private void testByteArrayPackingAndUnpacking( Machine machine, int length ) throws Throwable
    {
        byte[] array = new byte[length];

        machine.reset();
        machine.packer().pack( array );

        PackStream.Unpacker unpacker = newUnpacker( machine.output() );
        PackType packType = unpacker.peekNextType();

        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.BYTES ) );
        Assertions.assertArrayEquals( array, unpacker.unpackBytes() );
    }

    @Test
    void testCanPackAndUnpackStrings() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        for ( int i = 0; i < 24; i++ )
        {
            String string = new String( new byte[(int) Math.pow( 2, i )] );

            // When
            machine.reset();
            machine.packer().pack( string );

            // Then
            PackStream.Unpacker unpacker = newUnpacker( machine.output() );
            PackType packType = unpacker.peekNextType();

            // Then
            MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.STRING ) );
            MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( string ) );
        }

    }

    @Test
    void testCanPackAndUnpackBytes() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.pack( "ABCDEFGHIJ".getBytes() );

        // Then
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );
        PackType packType = unpacker.peekNextType();

        // Then
        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.BYTES ) );
        Assertions.assertArrayEquals( "ABCDEFGHIJ".getBytes(), unpacker.unpackBytes() );

    }

    @Test
    void testCanPackAndUnpackString() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.pack( "ABCDEFGHIJ" );

        // Then
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );
        PackType packType = unpacker.peekNextType();

        // Then
        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.STRING ) );
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "ABCDEFGHIJ" ));

    }

    @Test
    void testCanPackAndUnpackSpecialString() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        String code = "Mjölnir";

        // When
        PackStream.Packer packer = machine.packer();
        packer.pack( code );

        // Then
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );
        PackType packType = unpacker.peekNextType();

        // Then
        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.STRING ) );
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( code ));
    }

    @Test
    void testCanPackAndUnpackListOneItemAtATime() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packListHeader( 3 );
        packer.pack( 12 );
        packer.pack( 13 );
        packer.pack( 14 );

        // Then
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );
        PackType packType = unpacker.peekNextType();

        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.LIST ) );
        MatcherAssert.assertThat( unpacker.unpackListHeader(), CoreMatchers.equalTo( 3L ) );
        MatcherAssert.assertThat( unpacker.unpackLong(), CoreMatchers.equalTo( 12L ) );
        MatcherAssert.assertThat( unpacker.unpackLong(), CoreMatchers.equalTo( 13L ) );
        MatcherAssert.assertThat( unpacker.unpackLong(), CoreMatchers.equalTo( 14L ) );

    }

    @Test
    void testCanPackAndUnpackListOfString() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.pack( asList( "eins", "zwei", "drei" ) );

        // Then
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );
        PackType packType = unpacker.peekNextType();

        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.LIST ) );
        MatcherAssert.assertThat( unpacker.unpackListHeader(), CoreMatchers.equalTo( 3L ) );
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "eins" ) );
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "zwei" ) );
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "drei" ) );

    }

    @Test
    void testCanPackAndUnpackListOfSpecialStrings() throws Throwable
    {
        assertPackStringLists( 3, "Mjölnir" );
        assertPackStringLists( 126, "Mjölnir" );
        assertPackStringLists( 3000, "Mjölnir" );
        assertPackStringLists( 32768, "Mjölnir" );
    }

    @Test
    void testCanPackAndUnpackListOfStringOneByOne() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packListHeader( 3 );
        packer.pack( "eins" );
        packer.pack( "zwei" );
        packer.pack( "drei" );

        // Then
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );
        PackType packType = unpacker.peekNextType();

        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.LIST ) );
        MatcherAssert.assertThat( unpacker.unpackListHeader(), CoreMatchers.equalTo( 3L ) );
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "eins" ) );
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "zwei" ) );
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "drei" ) );

    }

    @Test
    void testCanPackAndUnpackListOfSpecialStringOneByOne() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packListHeader( 3 );
        packer.pack( "Mjölnir" );
        packer.pack( "Mjölnir" );
        packer.pack( "Mjölnir" );

        // Then
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );
        PackType packType = unpacker.peekNextType();

        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.LIST ) );
        MatcherAssert.assertThat( unpacker.unpackListHeader(), CoreMatchers.equalTo( 3L ) );
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "Mjölnir" ) );
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "Mjölnir" ) );
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "Mjölnir" ) );

    }

    @Test
    void testCanPackAndUnpackMap() throws Throwable
    {
        assertMap( 2 );
        assertMap( 126 );
        assertMap( 2439 );
        assertMap( 32768 );
    }

    @Test
    void testCanPackAndUnpackStruct() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packStructHeader( 3, (byte)'N' );
        packer.pack( 12 );
        packer.pack( asList( "Person", "Employee" ) );
        packer.pack( asMap( "name", "Alice", "age", 33 ) );

        // Then
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );
        PackType packType = unpacker.peekNextType();

        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.STRUCT ) );
        MatcherAssert.assertThat( unpacker.unpackStructHeader(), CoreMatchers.equalTo( 3L ) );
        MatcherAssert.assertThat( unpacker.unpackStructSignature(), CoreMatchers.equalTo( (byte)'N' ) );

        MatcherAssert.assertThat( unpacker.unpackLong(), CoreMatchers.equalTo( 12L ) );

        MatcherAssert.assertThat( unpacker.unpackListHeader(), CoreMatchers.equalTo( 2L ) );
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "Person" ));
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "Employee" ));

        MatcherAssert.assertThat( unpacker.unpackMapHeader(), CoreMatchers.equalTo( 2L ) );
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "name" ));
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "Alice" ));
        MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "age" ));
        MatcherAssert.assertThat( unpacker.unpackLong(), CoreMatchers.equalTo( 33L ) );
    }

    @Test
    void testCanPackAndUnpackStructsOfDifferentSizes() throws Throwable
    {
        assertStruct( 2 );
        assertStruct( 126 );
        assertStruct( 2439 );

        //we cannot have 'too many' fields
        Assertions.assertThrows( PackStream.Overflow.class, () -> assertStruct( 65536 ) );
    }

    @Test
    void testCanDoStreamingListUnpacking() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.pack( asList(1,2,3,asList(4,5)) );

        // When I unpack this value
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then I can do streaming unpacking
        long size = unpacker.unpackListHeader();
        long a = unpacker.unpackLong();
        long b = unpacker.unpackLong();
        long c = unpacker.unpackLong();

        long innerSize = unpacker.unpackListHeader();
        long d = unpacker.unpackLong();
        long e = unpacker.unpackLong();

        // And all the values should be sane
        Assertions.assertEquals( 4, size );
        Assertions.assertEquals( 2, innerSize );
        Assertions.assertEquals( 1, a );
        Assertions.assertEquals( 2, b );
        Assertions.assertEquals( 3, c );
        Assertions.assertEquals( 4, d );
        Assertions.assertEquals( 5, e );
    }

    @Test
    void testCanDoStreamingStructUnpacking() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.packStructHeader( 4, (byte)'~' );
        packer.pack( 1 );
        packer.pack( 2 );
        packer.pack( 3 );
        packer.pack( asList( 4,5 ) );

        // When I unpack this value
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then I can do streaming unpacking
        long size = unpacker.unpackStructHeader();
        byte signature = unpacker.unpackStructSignature();
        long a = unpacker.unpackLong();
        long b = unpacker.unpackLong();
        long c = unpacker.unpackLong();

        long innerSize = unpacker.unpackListHeader();
        long d = unpacker.unpackLong();
        long e = unpacker.unpackLong();

        // And all the values should be sane
        Assertions.assertEquals( 4, size );
        Assertions.assertEquals( '~', signature );
        Assertions.assertEquals( 2, innerSize );
        Assertions.assertEquals( 1, a );
        Assertions.assertEquals( 2, b );
        Assertions.assertEquals( 3, c );
        Assertions.assertEquals( 4, d );
        Assertions.assertEquals( 5, e );
    }

    @Test
    void testCanDoStreamingMapUnpacking() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.packMapHeader( 2 );
        packer.pack( "name" );
        packer.pack( "Bob" );
        packer.pack( "cat_ages" );
        packer.pack( asList( 4.3, true ) );

        // When I unpack this value
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then I can do streaming unpacking
        long size = unpacker.unpackMapHeader();
        String k1 = unpacker.unpackString();
        String v1 = unpacker.unpackString();
        String k2 = unpacker.unpackString();

        long innerSize = unpacker.unpackListHeader();
        double d = unpacker.unpackDouble();
        boolean e = unpacker.unpackBoolean();

        // And all the values should be sane
        Assertions.assertEquals( 2, size );
        Assertions.assertEquals( 2, innerSize );
        Assertions.assertEquals( "name", k1 );
        Assertions.assertEquals( "Bob", v1 );
        Assertions.assertEquals( "cat_ages", k2 );
        Assertions.assertEquals( 4.3, d, 0.0001 );
        Assertions.assertTrue( e );
    }

    @Test
    void handlesDataCrossingBufferBoundaries() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.pack( Long.MAX_VALUE );
        packer.pack( Long.MAX_VALUE );

        ReadableByteChannel ch = Channels.newChannel( new ByteArrayInputStream( machine.output() ) );
        PackStream.Unpacker unpacker = new PackStream.Unpacker( new BufferedChannelInput( 11, ch ) );

        // Serialized ch will look like, and misalign with the 11-byte unpack buffer:

        // [XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX]
        //  mkr \___________data______________/ mkr \___________data______________/
        // \____________unpack buffer_________________/

        // When
        long first = unpacker.unpackLong();
        long second = unpacker.unpackLong();

        // Then
        Assertions.assertEquals(Long.MAX_VALUE, first);
        Assertions.assertEquals(Long.MAX_VALUE, second);
    }

    @Test
    void testCanPeekOnNextType() throws Throwable
    {
        // When & Then
        assertPeekType( PackType.STRING, "a string" );
        assertPeekType( PackType.INTEGER, 123 );
        assertPeekType( PackType.FLOAT, 123.123 );
        assertPeekType( PackType.BOOLEAN, true );
        assertPeekType( PackType.LIST, asList( 1,2,3 ) );
        assertPeekType( PackType.MAP, asMap( "l",3 ) );
    }

    @Test
    void shouldFailForUnknownValue() throws IOException
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();

        // Expect
        Assertions.assertThrows( PackStream.UnPackable.class, () -> packer.pack( new MyRandomClass() ) );
    }

    private static class MyRandomClass{}

    private void assertPeekType( PackType type, Object value ) throws IOException
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.pack( value );

        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // When & Then
        Assertions.assertEquals( type, unpacker.peekNextType() );
    }

    private void assertPackStringLists( int size, String value ) throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        ArrayList<String> strings = new ArrayList<>( size );
        for ( int i = 0; i < size; i++ )
        {
            strings.add( i, value );
        }
        packer.pack( strings );

        // Then
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );
        PackType packType = unpacker.peekNextType();
        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.LIST ) );

        MatcherAssert.assertThat( unpacker.unpackListHeader(), CoreMatchers.equalTo( (long) size ) );
        for ( int i = 0; i < size; i++ )
        {
            MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( "Mjölnir" ) );
        }
    }

    private void assertMap( int size ) throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        HashMap<String,Integer> map = new HashMap<>();
        for ( int i = 0; i < size; i++ )
        {
            map.put( Integer.toString( i ), i );
        }
        packer.pack( map );

        // Then
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );
        PackType packType = unpacker.peekNextType();

        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.MAP ) );


        MatcherAssert.assertThat( unpacker.unpackMapHeader(), CoreMatchers.equalTo( (long) size ) );

        for ( int i = 0; i < size; i++ )
        {
            MatcherAssert.assertThat( unpacker.unpackString(), CoreMatchers.equalTo( Long.toString( unpacker.unpackLong() ) ) );
        }
    }

    private void assertStruct( int size ) throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packStructHeader( size, (byte) 'N' );
        for ( int i = 0; i < size; i++ )
        {
            packer.pack( i );
        }


        // Then
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );
        PackType packType = unpacker.peekNextType();

        MatcherAssert.assertThat( packType, CoreMatchers.equalTo( PackType.STRUCT ) );
        MatcherAssert.assertThat( unpacker.unpackStructHeader(), CoreMatchers.equalTo( (long) size ) );
        MatcherAssert.assertThat( unpacker.unpackStructSignature(), CoreMatchers.equalTo( (byte) 'N' ) );

        for ( int i = 0; i < size; i++ )
        {
            MatcherAssert.assertThat( unpacker.unpackLong(), CoreMatchers.equalTo( (long) i ) );
        }
    }
}
