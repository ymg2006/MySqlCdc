using MySqlCdc.Checksum;
using MySqlCdc.Commands;
using MySqlCdc.Constants;
using MySqlCdc.Network;
using MySqlCdc.Packets;
using MySqlCdc.Providers;
using System.Diagnostics.Metrics;

namespace MySqlCdc;

internal class Configurator(ReplicaOptions options, Connection connection, IDatabaseProvider databaseProvider)
{
    public async Task AdjustStartingPosition(CancellationToken cancellationToken = default)
    {
        if (options.Binlog.StartingStrategy != StartingStrategy.FromEnd)
            return;

        // Ignore if position was read before in case of reconnect.
        if (options.Binlog.Filename != string.Empty)
            return;

        var command = new QueryCommand($"show {(options.UseNewStatusCommand ? "binary log" : "master")} status");
        await connection.WritePacketAsync(command.Serialize(), 0, cancellationToken);

        var resultSet = await ReadResultSet(cancellationToken);
        if (resultSet.Count != 1)
            throw new InvalidOperationException("Could not read master binlog position.");

        options.Binlog.Filename = resultSet[0].Cells[0];
        options.Binlog.Position = long.Parse(resultSet[0].Cells[1]);
    }
    
    public async Task SetMasterHeartbeat(CancellationToken cancellationToken = default)
    {
        var milliseconds = (long)options.HeartbeatInterval.TotalMilliseconds;
        var nanoseconds = milliseconds * 1000 * 1000;
        var command = new QueryCommand($"set @master_heartbeat_period={nanoseconds}");
        await connection.WritePacketAsync(command.Serialize(), 0, cancellationToken);
        var (packet, _) = await connection.ReadPacketAsync(cancellationToken);
        Extensions.ThrowIfErrorPacket(packet, "Setting master_binlog_checksum error.");
    }

    public async Task SetMasterBinlogChecksum(CancellationToken cancellationToken = default)
    {
        var command = new QueryCommand("SET @master_binlog_checksum= @@global.binlog_checksum");
        await connection.WritePacketAsync(command.Serialize(), 0, cancellationToken);
        var (packet, _) = await connection.ReadPacketAsync(cancellationToken);
        Extensions.ThrowIfErrorPacket(packet, "Setting master_binlog_checksum error.");

        command = new QueryCommand("SELECT @master_binlog_checksum");
        await connection.WritePacketAsync(command.Serialize(), 0, cancellationToken);
        var resultSet = await ReadResultSet(cancellationToken);

        // When replication is started fake RotateEvent comes before FormatDescriptionEvent.
        // In order to deserialize the event we have to obtain checksum type length in advance.
        var checksumType = resultSet[0].Cells[0];
        databaseProvider.Deserializer.ChecksumStrategy = checksumType switch
        {
            "NONE" => new NoneChecksum(),
            "CRC32" => new Crc32Checksum(),
            _ => throw new InvalidOperationException("The master checksum type is not supported.")
        };
    }
    
    private async Task<List<ResultSetRowPacket>> ReadResultSet(CancellationToken cancellationToken = default)
    {
        var (packet, _) = await connection.ReadPacketAsync(cancellationToken);
        Extensions.ThrowIfErrorPacket(packet, "Reading result set error.");

        while (!cancellationToken.IsCancellationRequested)
        {
            // Skip through metadata
            (packet, _) = await connection.ReadPacketAsync(cancellationToken);
            if (packet[0] == (byte)ResponseType.EndOfFile)
                break;
        }

        var resultSet = new List<ResultSetRowPacket>();
        while (!cancellationToken.IsCancellationRequested)
        {
            (packet, _) = await connection.ReadPacketAsync(cancellationToken);
            Extensions.ThrowIfErrorPacket(packet, "Query result set error.");

            if (packet[0] == (byte)ResponseType.EndOfFile)
                break;

            resultSet.Add(new ResultSetRowPacket(packet));
        }
        return resultSet;
    }
}