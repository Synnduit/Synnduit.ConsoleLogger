using Synnduit.Events;
using Synnduit.Logging.Properties;

namespace Synnduit.Logging
{
    /// <summary>
    /// Logs individual run events to the console.
    /// </summary>
    /// <typeparam name="TEntity">The type representing the entity.</typeparam>
    [EventReceiver]
    public class ConsoleLogger<TEntity> : EventReceiver<TEntity>
        where TEntity : class
    {
        private const string AggregateResultsKey = "ConsoleLogger_AggregateResults";

        private const int RefreshInterval = 200;

        private readonly EntityTransactionOutcome[] entityTransactionOutcomes;

        private readonly Dictionary<EntityTransactionOutcome, int> aggregateResults;

        private readonly Dictionary<EntityTransactionOutcome, int> results;

        private bool initializingMessageWritten;

        private int entityCount;

        private int entitiesProcessed;

        private Dictionary<EntityTransactionOutcome, Coordinates> resultCoordinates;

        private Coordinates migrationProgressCoordinates;

        private int orphanMappingCount;

        private int orphanMappingsProcessed;

        private Coordinates orphanMappingsProgressCoordinates;

        private int entitiesDeleted;

        private Coordinates deletionProgressCoordinates;

        private DateTime lastRefreshDateTime;

        private DateTime segmentProcessingTime;

        /// <summary>
        /// Initializes a new instance of the class.
        /// </summary>
        public ConsoleLogger()
        {
            this.entityTransactionOutcomes = this.GetEntityTransactionOutcomes();
            this.aggregateResults = this.GetAggregateResults();
            this.results = this.CreateResults();
            this.initializingMessageWritten = false;
            this.entityCount = 0;
            this.resultCoordinates = null;
            this.migrationProgressCoordinates = null;

#pragma warning disable CA1416 // Validate platform compatibility
            Console.SetBufferSize(Console.BufferWidth, short.MaxValue - 1);
#pragma warning restore CA1416 // Validate platform compatibility
        }

        /// <summary>
        /// Called when the current run segment is about to be executed.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnSegmentExecuting(ISegmentExecutingArgs args)
        {
            this.segmentProcessingTime = DateTime.Now; //KHB
            Console.WriteLine();
            Console.WriteLine(
                Resources.SegmentExecuting,
                this.Context.SegmentIndex,
                this.Context.SegmentCount);
            Console.Write("  {0}: ", Resources.SegmentStartTime); //KHB
            this.PrintLine(segmentProcessingTime.ToString("o"), ConsoleColor.Cyan); //KHB
            Console.Write("  {0}: ", Resources.Type);
            this.PrintLine(this.GetLabel(this.Context.SegmentType), ConsoleColor.Cyan);
            if(this.Context.SourceSystem != null)
            {
                Console.Write("  {0}: ", Resources.SourceSystem);
                this.PrintLine(this.Context.SourceSystem.Name, ConsoleColor.Cyan);
            }
            Console.Write("  {0}: ", Resources.DestinationSystem);
            this.PrintLine(this.Context.DestinationSystem.Name, ConsoleColor.Cyan);
            Console.Write("  {0}: ", Resources.EntityType);
            this.PrintLine(this.Context.EntityType.Name, ConsoleColor.Cyan);
            Console.WriteLine();
        }

        /// <summary>
        /// Called when the current run segment finishes executing.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnSegmentExecuted(ISegmentExecutedArgs args)
        {
            Console.WriteLine(this.GetLabel(Resources.SegmentDuration), (DateTime.Now - this.segmentProcessingTime).TotalSeconds); //KHB
            if(this.Context.SegmentType == SegmentType.Migration)
            {
                this.entitiesProcessed = this.entityCount;
                if(this.entityCount > 0)
                {
                    this.PrintResults();
                }
            }
            Console.WriteLine();
            if(this.Context.SegmentIndex < this.Context.SegmentCount)
            {
                Console.WriteLine(" --- ");
            }
        }

        /// <summary>
        /// Called when a subsystem is about to be initialized.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnInitializing(IInitializingArgs args)
        {
            if(args.Message != null)
            {
                Console.Write("{0} ... ", args.Message);
                this.initializingMessageWritten = true;
            }
        }

        /// <summary>
        /// Called when a subsystem has been initialized.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnInitialized(IInitializedArgs args)
        {
            if(args.Message != null)
            {
                Console.WriteLine(args.Message);
            }
            else if(this.initializingMessageWritten)
            {
                Console.WriteLine();
            }
            this.initializingMessageWritten = false;
        }

        /// <summary>
        /// Called when source/destination system identifier mappings are about to be
        /// loaded from the database into the in-memory cache.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnMappingsCaching(IMappingsCachingArgs args)
        {
            Console.Write(Resources.LoadingEntityIdentifierMappings);
        }

        /// <summary>
        /// Called when source/destination system identifier mappings have been loaded from
        /// the database into the in-memory cache.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnMappingsCached(IMappingsCachedArgs args)
        {
            this.PrintVariableCountOutcome(
                args.Count,
                Resources.EntityIdentifierMappingLoaded,
                Resources.EntityIdentifierMappingsLoadedFormat);
        }

        /// <summary>
        /// Called when the (deduplication) in-memory cache of destination system entities
        /// is about to be populated.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnCachePopulating(ICachePopulatingArgs args)
        {
            Console.Write(Resources.CachingDestinationSystemEntities);
        }

        /// <summary>
        /// Called when the (deduplication) in-memory cache of destination system entities
        /// has been populated.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnCachePopulated(ICachePopulatedArgs args)
        {
            this.PrintVariableCountOutcome(
                args.Count,
                Resources.DestinationSystemEntityCached,
                Resources.DestinationSystemEntitiesCachedFormat);
        }

        /// <summary>
        /// Called when entities from the source system feed are about to be loaded.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnLoading(ILoadingArgs args)
        {
            Console.Write(Resources.Loading);
        }

        /// <summary>
        /// Called when entities from the source system have been loaded.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnLoaded(ILoadedArgs args)
        {
            this.entityCount = args.Count;
            this.PrintVariableCountOutcome(
                this.entityCount,
                Resources.EntityLoaded,
                Resources.EntitiesLoadedFormat);
        }

        /// <summary>
        /// Called when a source system entity is about to be processed.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnProcessing(IProcessingArgs<TEntity> args)
        {
            if(this.entitiesProcessed == 0)
            {
                this.PrintResultsTableLabels();
                this.PrintMigrationProgressLabel();
                this.PrintResults();
                this.lastRefreshDateTime = DateTime.Now;
            }
        }

        /// <summary>
        /// Called when a source system entity has been processed.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnProcessed(IProcessedArgs<TEntity> args)
        {
            this.results[args.Outcome]++;
            aggregateResults[args.Outcome]++;
            this.entitiesProcessed++;
            if((DateTime.Now - this.lastRefreshDateTime).TotalMilliseconds
                >= RefreshInterval
                || this.entitiesProcessed == this.entityCount)
            {
                this.PrintResults();
                this.lastRefreshDateTime = DateTime.Now;
            }
        }

        /// <summary>
        /// Called when orphan identifier mappings are about to be processed.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnOrphanMappingsProcessing(IOrphanMappingsProcessingArgs args)
        {
            this.orphanMappingCount = args.Count;
            this.orphanMappingsProcessed = 0;
            this.lastRefreshDateTime = DateTime.Now;
            this.PrintOrphanMappingsProgressLabel(args);
        }

        /// <summary>
        /// Called when an orphan mapping has been processed.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnOrphanMappingProcessed(IOrphanMappingProcessedArgs args)
        {
            this.orphanMappingsProcessed++;
            if((DateTime.Now - this.lastRefreshDateTime).TotalMilliseconds
                >= RefreshInterval
                || this.orphanMappingsProcessed == this.orphanMappingCount)
            {
                this.PrintOrphanMappingsProgress();
                this.lastRefreshDateTime = DateTime.Now;
            }
        }

        /// <summary>
        /// Called when a garbage collection run segment has been initialized.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnGarbageCollectionInitializing(
            IGarbageCollectionInitializingArgs args)
        {
            Console.Write(Resources.IdentifyingEntitiesToDelete);
        }

        /// <summary>
        /// Called when a garbage collection run segment has been initialized.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnGarbageCollectionInitialized(
            IGarbageCollectionInitializedArgs args)
        { 
            this.entityCount = args.Count;
            this.entitiesDeleted = 0;
            this.lastRefreshDateTime = DateTime.Now;
            this.PrintVariableCountOutcome(
                args.Count,
                Resources.EntityIdentified,
                Resources.EntitiesIdentifiedFormat);
            if(this.entityCount > 0)
            {
                this.PrintEntityDeletionProgressLabel();
            }
        }

        /// <summary>
        /// Called when the deletion of a destination system entity (identified for
        /// deletion) has been processed.
        /// </summary>
        /// <param name="args">The event data.</param>
        public override void OnDeletionProcessed(IDeletionProcessedArgs args)
        {
            this.entitiesDeleted++;
            if((DateTime.Now - this.lastRefreshDateTime).TotalMilliseconds
                >= RefreshInterval
                || this.entitiesDeleted == this.entityCount)
            {
                this.PrintEntityDeletionProgress();
                this.lastRefreshDateTime = DateTime.Now;
            }
        }

        private void PrintVariableCountOutcome(
            int count,
            string singleItemMessage,
            string multipleItemsMessageFormat)
        {
            if(count != 1)
            {
                Console.WriteLine(multipleItemsMessageFormat, count);
            }
            else
            {
                Console.WriteLine(singleItemMessage);
            }
        }

        private EntityTransactionOutcome[] GetEntityTransactionOutcomes()
        {
            return
                Enum.GetValues(typeof(EntityTransactionOutcome))
                .Cast<EntityTransactionOutcome>()
                .OrderBy(outcome => (int) outcome)
                .ToArray();
        }

        private Dictionary<EntityTransactionOutcome, int> GetAggregateResults()
        {
            Dictionary<EntityTransactionOutcome, int> aggregateResults;
            this.Context.RunData.TryGetValue(
                AggregateResultsKey, out object runDataValue);
            aggregateResults = runDataValue as Dictionary<EntityTransactionOutcome, int>;
            if(aggregateResults == null)
            {
                aggregateResults = this.CreateResults();
                this.Context.RunData[AggregateResultsKey] = aggregateResults;
            }
            return aggregateResults;
        }

        private Dictionary<EntityTransactionOutcome, int> CreateResults()
        {
            var results = new Dictionary<EntityTransactionOutcome, int>();
            foreach(EntityTransactionOutcome outcome in this.entityTransactionOutcomes)
            {
                results.Add(outcome, 0);
            }
            return results;
        }

        private void PrintLine(string text, ConsoleColor color)
        {
            Console.ForegroundColor = color;
            Console.WriteLine(text);
            Console.ResetColor();
        }

        private void PrintResultsTableLabels()
        {
            Console.WriteLine();
            this.resultCoordinates =
                new Dictionary<EntityTransactionOutcome, Coordinates>();
            foreach(EntityTransactionOutcome outcome in this.entityTransactionOutcomes)
            {
                Console.Write("{0}: ", this.GetLabel(outcome));
                this.resultCoordinates.Add(outcome, Coordinates.GetCurrent());
                Console.WriteLine();
            }
            Console.WriteLine();
        }

        private string GetLabel<T>(T value)
        {
            string label = Resources.ResourceManager.GetString(value.ToString());
            if(label == null)
            {
                label = value.ToString();
            }
            return label;
        }

        private void PrintMigrationProgressLabel()
        {
            Console.Write("{0}: ", Resources.MigrationProgress);
            this.migrationProgressCoordinates = Coordinates.GetCurrent();
            Console.WriteLine();
        }

        private void PrintResults()
        {
            foreach(EntityTransactionOutcome outcome in this.entityTransactionOutcomes)
            {
                this.PrintResult(outcome);
            }
            this.PrintMigrationProgress();
        }

        private void PrintResult(EntityTransactionOutcome outcome)
        {
            int currentResult = this.results[outcome];
            int aggregateResult = aggregateResults[outcome];
            if(currentResult > 0 || aggregateResult > 0)
            {
                Coordinates coordinates = this.resultCoordinates[outcome];
                this.PrintResult(coordinates, currentResult);
                coordinates.Print(" ( ");
                this.PrintResult(coordinates, aggregateResult);
                coordinates.PrintAndReturn(" ) ");
            }
        }

        private void PrintResult(Coordinates coordinates, int result)
        {
            coordinates.Print(
                result.ToString(Resources.ResultFormat),
                result > 0 ? (ConsoleColor?) ConsoleColor.Yellow : null);
        }

        private void PrintMigrationProgress()
        {
            decimal migrationProgress =
                (this.entitiesProcessed * 100.0m) / this.entityCount;
            this.migrationProgressCoordinates.PrintAndReturn(
                string.Format(Resources.ProgressFormat, migrationProgress),
                ConsoleColor.Green);
        }

        private void PrintOrphanMappingsProgressLabel(IOrphanMappingsProcessingArgs args)
        {
            Console.Write(this.GetOrphanMappingsLabel(args));
            this.orphanMappingsProgressCoordinates = Coordinates.GetCurrent();
            Console.WriteLine();
            this.PrintOrphanMappingsProgress();
        }

        private string GetOrphanMappingsLabel(IOrphanMappingsProcessingArgs args)
        {
            string format = Resources.ResourceManager.GetString(
                string.Format("{0}OrphanMappings", args.Behavior));
            if(format == null)
            {
                format = "{0:#,##0}";
            }
            return string.Format(format, args.Count);
        }

        private void PrintOrphanMappingsProgress()
        {
            decimal orphanMappingsProgress =
                (this.orphanMappingsProcessed * 100.0m) / this.orphanMappingCount;
            this.orphanMappingsProgressCoordinates.PrintAndReturn(
                string.Format(Resources.ProgressFormat, orphanMappingsProgress),
                ConsoleColor.Green);
        }

        private void PrintEntityDeletionProgressLabel()
        {
            Console.WriteLine();
            Console.Write("{0}: ", Resources.DeletionProgress);
            this.deletionProgressCoordinates = Coordinates.GetCurrent();
            Console.WriteLine();
        }

        private void PrintEntityDeletionProgress()
        {
            decimal deletionProgress =
                (this.entitiesDeleted * 100.00m) / this.entityCount;
            this.deletionProgressCoordinates.PrintAndReturn(
                string.Format(Resources.ProgressFormat, deletionProgress),
                ConsoleColor.Green);
        }

        private class Coordinates
        {
            private readonly int left;

            private readonly int top;

            private int offset;

            public static Coordinates GetCurrent()
            {
                return new Coordinates(Console.CursorLeft, Console.CursorTop);
            }

            private Coordinates(int left, int top)
            {
                this.left = left;
                this.top = top;
                this.offset = 0;
            }

            public void Print(string text, ConsoleColor? color = null)
            {
                Console.CursorVisible = false;
                int currentLeft = Console.CursorLeft;
                int currentTop = Console.CursorTop;
                Console.SetCursorPosition(this.left + this.offset, this.top);
                this.offset += text.Length;
                if(color.HasValue)
                {
                    Console.ForegroundColor = color.Value;
                }
                Console.Write(text);
                Console.ResetColor();
                Console.SetCursorPosition(currentLeft, currentTop);
                Console.CursorVisible = true;
            }

            public void PrintAndReturn(string text, ConsoleColor? color = null)
            {
                this.Print(text, color);
                this.offset = 0;
            }
        }
    }
}
