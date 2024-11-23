using MySqlCdc.Events;

namespace MySqlCdc.Providers.MariaDb;

/// <summary>
/// Represents GtidList from MariaDB.
/// </summary>
public class GtidList : IGtidState
{
    /// <summary>
    /// Gets a list of Gtids per each domain.
    /// </summary>
    public List<Gtid> Gtids { get; } = new();

    /// <summary>
    /// Parses <see cref="GtidList"/> from string representation.
    /// </summary>
    public static GtidList Parse(string gtidList)
    {
        if (gtidList == string.Empty)
            return new GtidList();

        var gtids = gtidList.Replace("\n", string.Empty)
            .Split(',')
            .Select(x => x.Trim())
            .ToArray();

        var domainMap = new HashSet<long>();
        var result = new GtidList();
        foreach (var gtid in gtids)
        {
            var components = gtid.Split('-');
            var domainId = long.Parse(components[0]);
            var serverId = long.Parse(components[1]);
            var sequence = long.Parse(components[2]);

            if (domainMap.Contains(domainId))
            {
                throw new FormatException("GtidList must consist of unique domain ids");
            }

            domainMap.Add(domainId);

            result.Gtids.Add(new Gtid(domainId, serverId, sequence));
        }
        return result;
    }

    /// <summary>
    /// Adds a gtid value to the GtidList.
    /// </summary>
    public bool AddGtid(IGtid gtidRaw)
    {
        var gtid = (Gtid)gtidRaw;

        for (var i = 0; i < Gtids.Count; i++)
        {
            if (Gtids[i].DomainId == gtid.DomainId)
            {
                Gtids[i] = gtid;
                return false;
            }
        }

        Gtids.Add(gtid);
        return true;
    }

    /// <summary>
    /// Returns string representation of the GtidList.
    /// </summary>
    public override string ToString() => string.Join(",", Gtids);
}