using MySqlCdc.Columns;
using MySqlCdc.Protocol;
using MySqlCdc.Providers.MySql;
using Newtonsoft.Json.Linq;
using Xunit;

namespace MySqlCdc.Tests.Providers;

public class ColumnParserTests
{
    private const string SkipReason = "https://stackoverflow.com/a/60084985/23633";
    private const string TestString = "Lorem ipsum dolor sit amet";

    [Fact]
    public void Test_TinyInt_Max()
    {
        var payload = new byte[] { 127 };
        var reader = new PacketReader(payload);
        Assert.Equal(127, (sbyte)ColumnParser.ParseTinyInt(ref reader, 0));
        Assert.Equal(1, reader.Consumed);
    }

    [Fact]
    public void Test_TinyInt_Min()
    {
        var payload = new byte[] { 128 };
        var reader = new PacketReader(payload);
        Assert.Equal(-128, (sbyte)ColumnParser.ParseTinyInt(ref reader, 0));
        Assert.Equal(1, reader.Consumed);
    }

    [Fact]
    public void Test_SmallInt_Max()
    {
        var payload = new byte[] { 255, 127 };
        var reader = new PacketReader(payload);
        Assert.Equal(32767, ColumnParser.ParseSmallInt(ref reader, 0));
        Assert.Equal(2, reader.Consumed);
    }

    [Fact]
    public void Test_SmallInt_Min()
    {
        var payload = new byte[] { 0, 128 };
        var reader = new PacketReader(payload);
        Assert.Equal(-32768, ColumnParser.ParseSmallInt(ref reader, 0));
        Assert.Equal(2, reader.Consumed);
    }

    [Fact]
    public void Test_MediumInt_Max()
    {
        var payload = new byte[] { 255, 255, 127 };
        var reader = new PacketReader(payload);
        Assert.Equal(8388607, ColumnParser.ParseMediumInt(ref reader, 0));
        Assert.Equal(3, reader.Consumed);
    }

    [Fact]
    public void Test_MediumInt_Min()
    {
        var payload = new byte[] { 0, 0, 128 };
        var reader = new PacketReader(payload);
        Assert.Equal(-8388608, ColumnParser.ParseMediumInt(ref reader, 0));
        Assert.Equal(3, reader.Consumed);
    }

    [Fact]
    public void Test_Int_Max()
    {
        var payload = new byte[] { 255, 255, 255, 127 };
        var reader = new PacketReader(payload);
        Assert.Equal(2147483647, ColumnParser.ParseInt(ref reader, 0));
        Assert.Equal(4, reader.Consumed);
    }

    [Fact]
    public void Test_Int_Min()
    {
        var payload = new byte[] { 0, 0, 0, 128 };
        var reader = new PacketReader(payload);
        Assert.Equal(-2147483648, ColumnParser.ParseInt(ref reader, 0));
        Assert.Equal(4, reader.Consumed);
    }

    [Fact]
    public void Test_BigInt_Max()
    {
        var payload = new byte[] { 255, 255, 255, 255, 255, 255, 255, 127 };
        var reader = new PacketReader(payload);
        Assert.Equal(9223372036854775807, ColumnParser.ParseBigInt(ref reader, 0));
        Assert.Equal(8, reader.Consumed);
    }

    [Fact]
    public void Test_BigInt_Min()
    {
        var payload = new byte[] { 0, 0, 0, 0, 0, 0, 0, 128 };
        var reader = new PacketReader(payload);
        Assert.Equal(-9223372036854775808, ColumnParser.ParseBigInt(ref reader, 0));
        Assert.Equal(8, reader.Consumed);
    }

    [Fact(Skip = SkipReason)]
    public void Test_Float_Positive()
    {
        var payload = new byte[] { 121, 233, 246, 66 };
        var reader = new PacketReader(payload);
        Assert.Equal(123.456, ColumnParser.ParseFloat(ref reader, 0));
        Assert.Equal(4, reader.Consumed);
    }

    [Fact(Skip = SkipReason)]
    public void Test_Float_Negative()
    {
        var payload = new byte[] { 121, 233, 246, 194 };
        var reader = new PacketReader(payload);
        Assert.Equal(-123.456, ColumnParser.ParseFloat(ref reader, 0));
        Assert.Equal(4, reader.Consumed);
    }

    [Fact]
    public void Test_Double_Positive()
    {
        var payload = new byte[] { 196, 34, 101, 84, 52, 111, 157, 65 };
        var reader = new PacketReader(payload);
        Assert.Equal(123456789.09876543, ColumnParser.ParseDouble(ref reader, 0));
        Assert.Equal(8, reader.Consumed);
    }

    [Fact]
    public void Test_Double_Negative()
    {
        var payload = new byte[] { 196, 34, 101, 84, 52, 111, 157, 193 };
        var reader = new PacketReader(payload);
        Assert.Equal(-123456789.09876543, ColumnParser.ParseDouble(ref reader, 0));
        Assert.Equal(8, reader.Consumed);
    }

    [Fact]
    public void Test_String_MetadataLess256()
    {
        // varchar(30), DEFAULT CHARSET=utf8mb4
        var payload = new byte[]
        {
            26, 76, 111, 114, 101, 109, 32, 105, 112, 115, 117, 109, 32, 100, 111, 108, 111, 114, 32, 115, 105, 116, 32,
            97, 109, 101, 116
        };
        var metadata = 120; // 30 * sizeof(utf8mb4)
        var reader = new PacketReader(payload);
        Assert.Equal(TestString, ColumnParser.ParseString(ref reader, metadata));
        Assert.Equal(sizeof(byte) + TestString.Length, reader.Consumed);
    }

    [Fact]
    public void Test_String_MetadataMore256()
    {
        // varchar(100), DEFAULT CHARSET=utf8mb4
        var payload = new byte[]
        {
            26, 0, 76, 111, 114, 101, 109, 32, 105, 112, 115, 117, 109, 32, 100, 111, 108, 111, 114, 32, 115, 105, 116,
            32, 97, 109, 101, 116
        };
        var metadata = 400; // 100 * sizeof(utf8mb4)
        var reader = new PacketReader(payload);
        Assert.Equal(TestString, ColumnParser.ParseString(ref reader, metadata));
        Assert.Equal(sizeof(short) + TestString.Length, reader.Consumed);
    }

    [Fact]
    public void Test_Blob_Array()
    {
        // json is also blob
        var payload = new byte[]
        {
            90, 1, 0, 0, /* Json starts here */ 0, 19, 0, 89, 1, 137, 0, 3, 0, 140, 0, 3, 0, 143, 0, 4, 0, 147, 0, 4, 0,
            151, 0, 4, 0, 155, 0, 6, 0, 161, 0, 6, 0, 167, 0, 6, 0, 173, 0, 7, 0, 180, 0, 7, 0, 187, 0, 7, 0, 194, 0, 8,
            0, 202, 0, 8, 0, 210, 0, 8, 0, 218, 0, 12, 0, 230, 0, 12, 0, 242, 0, 12, 0, 254, 0, 13, 0, 11, 1, 13, 0, 5,
            0, 0, 5, 1, 0, 5, 255, 255, 2, 24, 1, 0, 28, 1, 11, 32, 1, 4, 0, 0, 4, 1, 0, 5, 255, 127, 7, 40, 1, 4, 2, 0,
            5, 0, 128, 7, 44, 1, 12, 48, 1, 7, 55, 1, 9, 59, 1, 2, 67, 1, 7, 77, 1, 9, 81, 1, 107, 46, 48, 107, 46, 49,
            107, 46, 45, 49, 107, 46, 91, 93, 107, 46, 123, 125, 107, 46, 51, 46, 49, 52, 107, 46, 110, 117, 108, 108,
            107, 46, 116, 114, 117, 101, 107, 46, 51, 50, 55, 54, 55, 107, 46, 51, 50, 55, 54, 56, 107, 46, 102, 97,
            108, 115, 101, 107, 46, 45, 51, 50, 55, 54, 56, 107, 46, 45, 51, 50, 55, 54, 57, 107, 46, 115, 116, 114,
            105, 110, 103, 107, 46, 50, 49, 52, 55, 52, 56, 51, 54, 52, 55, 107, 46, 50, 49, 52, 55, 52, 56, 51, 54, 52,
            56, 107, 46, 116, 114, 117, 101, 95, 102, 97, 108, 115, 101, 107, 46, 45, 50, 49, 52, 55, 52, 56, 51, 54,
            52, 56, 107, 46, 45, 50, 49, 52, 55, 52, 56, 51, 54, 52, 57, 0, 0, 4, 0, 0, 0, 4, 0, 31, 133, 235, 81, 184,
            30, 9, 64, 0, 128, 0, 0, 255, 127, 255, 255, 6, 115, 116, 114, 105, 110, 103, 255, 255, 255, 127, 0, 0, 0,
            128, 0, 0, 0, 0, 2, 0, 10, 0, 4, 1, 0, 4, 2, 0, 0, 0, 0, 128, 255, 255, 255, 127, 255, 255, 255, 255
        };
        var expected =
            "{\"k.1\":1,\"k.0\":0,\"k.-1\":-1,\"k.true\":true,\"k.false\":false,\"k.null\":null,\"k.string\":\"string\",\"k.true_false\":[true,false],\"k.32767\":32767,\"k.32768\":32768,\"k.-32768\":-32768,\"k.-32769\":-32769,\"k.2147483647\":2147483647,\"k.2147483648\":2147483648,\"k.-2147483648\":-2147483648,\"k.-2147483649\":-2147483649,\"k.3.14\":3.14,\"k.{}\":{},\"k.[]\":[]}";
        var metadata = 4;
        var reader = new PacketReader(payload);
        var json = JsonParser.Parse(ColumnParser.ParseBlob(ref reader, metadata));
        Assert.True(JToken.DeepEquals(JToken.Parse(json), JToken.Parse(expected)));
        Assert.Equal(payload.Length, reader.Consumed);
    }

    [Fact]
    public void Test_Bit()
    {
        // bit(28), column = b'0111101101010101011110000111', select bin(column) from table
        var payload = new byte[] { 7, 181, 87, 135 };
        var metadata = 772;
        var reader = new PacketReader(payload);
        var expected = "0111101101010101011110000111".Select(x => x == '1').ToArray();
        var actual = ColumnParser.ParseBit(ref reader, metadata);
        Assert.Equal(expected, actual);
        Assert.Equal(payload.Length, reader.Consumed);
    }

    [Fact]
    public void Test_Enum_Value()
    {
        // enum('Low', 'Medium', 'High'), column = 'High', maximum 65535 elements
        var payload = new byte[] { 3 };
        var metadata = 1;
        var reader = new PacketReader(payload);
        Assert.Equal(3, ColumnParser.ParseEnum(ref reader, metadata));
        Assert.Equal(metadata, reader.Consumed);
    }

    [Fact]
    public void Test_Set_Value()
    {
        // set('Green', 'Yellow', 'Red'), column = 'Yellow,Red', maximum 64 bits
        var payload = new byte[] { 6 };
        var metadata = 1;
        var reader = new PacketReader(payload);
        Assert.Equal((1 << 1) | (1 << 2), ColumnParser.ParseSet(ref reader, metadata));
        Assert.Equal(metadata, reader.Consumed);
    }

    [Fact]
    public void Test_Year_Min()
    {
        var payload = new byte[] { 1 };
        var reader = new PacketReader(payload);
        Assert.Equal(1901, ColumnParser.ParseYear(ref reader, 0));
        Assert.Equal(1, reader.Consumed);
    }

    [Fact]
    public void Test_Year_Max()
    {
        var payload = new byte[] { 255 };
        var reader = new PacketReader(payload);
        Assert.Equal(2155, ColumnParser.ParseYear(ref reader, 0));
        Assert.Equal(1, reader.Consumed);
    }

    [Fact]
    public void Test_Date()
    {
        var payload = new byte[] { 87, 131, 15 };
        var reader = new PacketReader(payload);
        Assert.Equal(new DateOnly(1985, 10, 23), ColumnParser.ParseDate(ref reader, 0));
        Assert.Equal(3, reader.Consumed);
    }

    [Fact]
    public void Test_Date_Invalid()
    {
        var payload = new byte[] { 0, 0, 0 };
        var reader = new PacketReader(payload);
        Assert.Null(ColumnParser.ParseDate(ref reader, 0));
        Assert.Equal(3, reader.Consumed);
    }

    [Fact]
    public void Test_Time2_Positive()
    {
        // time(2), column = '15:22:33.67'
        var payload = new byte[] { 128, 245, 161, 67 };
        var reader = new PacketReader(payload);
        var metadata = 2;
        Assert.Equal(new TimeSpan(0, 15, 22, 33, 670), ColumnParser.ParseTime2(ref reader, metadata));
        Assert.Equal(4, reader.Consumed);
    }

    [Fact]
    public void Test_Time2_Positive_Len4()
    {
        // time(2), column = '15:22:33.1234'
        var payload = new byte[] { 128, 245, 161, 4, 210 };
        var reader = new PacketReader(payload);
        var metadata = 4;

        var expected = new TimeSpan(0, 15, 22, 33);
        expected = expected.Add(TimeSpan.FromMilliseconds(123.4));
        Assert.Equal(expected, ColumnParser.ParseTime2(ref reader, metadata));
        Assert.Equal(5, reader.Consumed);
    }

    [Fact]
    public void Test_Time2_Positive_Len4_with_frac_zero()
    {
        // time(2), column = '15:22:33.0000'
        var payload = new byte[] { 128, 245, 161, 0, 0 };
        var reader = new PacketReader(payload);
        var metadata = 4;

        var expected = new TimeSpan(0, 15, 22, 33);
        expected = expected.Add(TimeSpan.FromMilliseconds(000.0));
        Assert.Equal(expected, ColumnParser.ParseTime2(ref reader, metadata));
        Assert.Equal(5, reader.Consumed);
    }

    [Fact]
    public void Test_Time2_Positive_Len6()
    {
        // time(2), column = '15:22:33.123456'
        var payload = new byte[] { 128, 245, 161, 1, 226, 64 };
        var reader = new PacketReader(payload);
        var metadata = 6;

        var expected = new TimeSpan(0, 15, 22, 33);
        expected = expected.Add(TimeSpan.FromMilliseconds(123.456));
        Assert.Equal(expected, ColumnParser.ParseTime2(ref reader, metadata));
        Assert.Equal(6, reader.Consumed);
    }

    [Fact]
    public void Test_Time2_Negative_without_fraction()
    {
        // time(2), column = '-11:05:10'
        var payload = new byte[] { 127, 78, 182 };
        var reader = new PacketReader(payload);
        var metadata = 0;

        Assert.Equal(new TimeSpan(0, 11, 05, 10, 0).Negate(), ColumnParser.ParseTime2(ref reader, metadata));
        Assert.Equal(3, reader.Consumed);
    }

    [Fact]
    public void Test_Time2_Negative()
    {
        // time(2), column = '-15:22:33.67'
        var payload = new byte[] { 127, 10, 94, 189 };
        var reader = new PacketReader(payload);
        var metadata = 2;
        Assert.Equal(new TimeSpan(0, 15, 22, 33, 670).Negate(), ColumnParser.ParseTime2(ref reader, metadata));
        Assert.Equal(4, reader.Consumed);
    }

    [Fact]
    public void Test_Time2_Negative_Len_3()
    {
        // time(2), column = '-15:22:33.123'
        var payload = new byte[] { 127, 10, 94, 251, 50 };
        var reader = new PacketReader(payload);
        var metadata = 4;

        var expected = new TimeSpan(0, 15, 22, 33);
        expected = expected.Add(TimeSpan.FromMilliseconds(123));
        Assert.Equal(expected.Negate(), ColumnParser.ParseTime2(ref reader, metadata));
        Assert.Equal(5, reader.Consumed);
    }

    [Fact]
    public void Test_Time2_Negative_Len_4()
    {
        // time(2), column = '-15:22:33.1234'
        var payload = new byte[] { 127, 10, 94, 251, 46 };
        var reader = new PacketReader(payload);
        var metadata = 4;

        var expected = new TimeSpan(0, 15, 22, 33);
        expected = expected.Add(TimeSpan.FromMilliseconds(123.4));
        Assert.Equal(expected.Negate(), ColumnParser.ParseTime2(ref reader, metadata));
        Assert.Equal(5, reader.Consumed);
    }

    [Fact]
    public void Test_Time2_Negative_Len_4_with_frac_zero()
    {
        // time(2), column = '-15:22:33.0000'
        var payload = new byte[] { 127, 10, 95, 0, 0 };
        var reader = new PacketReader(payload);
        var metadata = 4;

        var expected = new TimeSpan(0, 15, 22, 33);
        expected = expected.Add(TimeSpan.FromMilliseconds(000.0));
        Assert.Equal(expected.Negate(), ColumnParser.ParseTime2(ref reader, metadata));
        Assert.Equal(5, reader.Consumed);
    }

    [Fact]
    public void Test_Time2_Negative_Len_5()
    {
        // time(2), column = '-15:22:33.12345'
        var payload = new byte[] { 127, 10, 94, 254, 29, 198 };
        var reader = new PacketReader(payload);
        var metadata = 6;

        var expected = new TimeSpan(0, 15, 22, 33);
        expected = expected.Add(TimeSpan.FromMilliseconds(123.45));
        Assert.Equal(expected.Negate(), ColumnParser.ParseTime2(ref reader, metadata));
        Assert.Equal(6, reader.Consumed);
    }

    [Fact]
    public void Test_Time2_Negative_Len_6()
    {
        // time(2), column = '-15:22:33.123456'
        var payload = new byte[] { 127, 10, 94, 254, 29, 192 };
        var reader = new PacketReader(payload);
        var metadata = 6;

        var expected = new TimeSpan(0, 15, 22, 33);
        expected = expected.Add(TimeSpan.FromMilliseconds(123.456));
        Assert.Equal(expected.Negate(), ColumnParser.ParseTime2(ref reader, metadata));
        Assert.Equal(6, reader.Consumed);
    }

    [Fact]
    public static void Test_TimeStamp2()
    {
        // timestamp(3), column = '1985-10-13 13:22:23.567' - was set in Ukraine (GMT+3)
        var payload = new byte[] { 29, 175, 151, 223, 22, 38 };
        var reader = new PacketReader(payload);
        var metadata = 3;
        var actual = ColumnParser.ParseTimeStamp2(ref reader, metadata);
        Assert.Equal(new DateTimeOffset(1985, 10, 13, 10, 22, 23, 567, TimeSpan.Zero), actual);
        Assert.Equal(new DateTimeOffset(1985, 10, 13, 13, 22, 23, 567, TimeSpan.FromHours(3)), actual);
        Assert.Equal(6, reader.Consumed);
    }

    [Fact]
    public void Test_DateTime2()
    {
        // datetime(3), column = '1988-10-14 14:12:13.345'
        var payload = new byte[] { 153, 63, 156, 227, 13, 13, 122 };
        var reader = new PacketReader(payload);
        var metadata = 3;
        Assert.Equal(new DateTime(1988, 10, 14, 14, 12, 13, 345, DateTimeKind.Utc),
            ColumnParser.ParseDateTime2(ref reader, metadata));
        Assert.Equal(7, reader.Consumed);
    }

    [Fact]
    public void Test_DateTime2_Invalid()
    {
        // datetime(3), 0000-00-00
        var payload = new byte[] { 0, 0, 0, 0, 0, 13, 122 };
        var reader = new PacketReader(payload);
        var metadata = 3;
        Assert.Null(ColumnParser.ParseDateTime2(ref reader, metadata));
        Assert.Equal(7, reader.Consumed);
    }

    #region MYSQL5_5_DATETIME_FORMAT - These tests were reproduced in MariaDB 10.4 by setting mysql56_temporal_format = OFF

    [Fact]
    public void Test_Time_Positive()
    {
        // time, column = '14:12:13'
        var payload = new byte[] { 157, 39, 2 };
        var reader = new PacketReader(payload);
        Assert.Equal(new TimeSpan(0, 14, 12, 13), ColumnParser.ParseTime(ref reader, 0));
        Assert.Equal(3, reader.Consumed);
    }

    [Fact(Skip = "See ParseTime method implementation")]
    public void Test_Time_Negative()
    {
        // time, column = '-14:12:13'
        var payload = new byte[] { 99, 216, 253 };
        var reader = new PacketReader(payload);
        Assert.Equal(new TimeSpan(0, -14, 12, 13), ColumnParser.ParseTime2(ref reader, 0));
        Assert.Equal(3, reader.Consumed);
    }

    [Fact]
    public void Test_TimeStamp()
    {
        // timestamp, column = '1985-10-13 13:22:23' - was set in Ukraine (GMT+3)
        var payload = new byte[] { 223, 151, 175, 29 };
        var reader = new PacketReader(payload);
        var actual = ColumnParser.ParseTimeStamp(ref reader, 0);
        Assert.Equal(new DateTimeOffset(1985, 10, 13, 10, 22, 23, TimeSpan.Zero), actual);
        Assert.Equal(new DateTimeOffset(1985, 10, 13, 13, 22, 23, TimeSpan.FromHours(3)), actual);
        Assert.Equal(4, reader.Consumed);
    }

    [Fact]
    public void Test_DateTime()
    {
        // datetime, column = '1988-10-16 14:12:13'
        var payload = new byte[] { 157, 165, 231, 232, 20, 18, 0, 0 };
        var reader = new PacketReader(payload);
        var metadata = 3;
        Assert.Equal(new DateTime(1988, 10, 16, 14, 12, 13, DateTimeKind.Utc),
            ColumnParser.ParseDateTime(ref reader, metadata));
        Assert.Equal(8, reader.Consumed);
    }

    [Fact]
    public void Test_DateTime_Invalid()
    {
        // datetime, 0000-00-00
        var payload = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
        var reader = new PacketReader(payload);
        var metadata = 3;
        Assert.Null(ColumnParser.ParseDateTime(ref reader, metadata));
        Assert.Equal(8, reader.Consumed);
    }
}

#endregion
