using System.Collections.Generic;

namespace MySql.Cdc.Events
{
    /// <summary>
    /// Represents an inserted or deleted row in row based replication.
    /// </summary>
    public class ColumnData
    {
        /// <summary>
        /// Column values of the changed row.
        /// </summary>
        public IReadOnlyList<object> Cells { get; }

        public ColumnData(IReadOnlyList<object> cells)
        {
            Cells = cells;
        }
    }

    /// <summary>
    /// Represents an updated row in row based replication.
    /// </summary>
    public class UpdateColumnData
    {
        /// <summary>
        /// Row state before it was updated.
        /// </summary>
        public ColumnData BeforeUpdate { get; }

        /// <summary>
        /// Actual row state after update.
        /// </summary>
        public ColumnData AfterUpdate { get; }

        public UpdateColumnData(ColumnData beforeUpdate, ColumnData afterUpdate)
        {
            BeforeUpdate = beforeUpdate;
            AfterUpdate = afterUpdate;
        }
    }
}
