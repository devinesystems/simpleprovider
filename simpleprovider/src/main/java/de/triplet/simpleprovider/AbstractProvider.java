package de.triplet.simpleprovider;

import android.content.ContentProvider;
import android.content.ContentProviderOperation;
import android.content.ContentProviderResult;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.OperationApplicationException;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteConstraintException;
import android.database.sqlite.SQLiteCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.net.Uri;
import android.provider.BaseColumns;
import android.util.Log;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused") // Public API
public abstract class AbstractProvider extends ContentProvider {

    protected final String mLogTag;
    protected SQLiteDatabase mDatabase;
    public static final String QUERY_CALLER_IS_SYNC_ADAPTER = "caller_is_sync_adapter";
    public static final String QUERY_CONFLICT_ALGORITHM = "conflictAlgorithm";
    private static final String PARAM_TRUE = "1";
    protected final Object mMatcherSynchronizer;
    protected UriMatcher mMatcher;
    protected MatchDetail[] mMatchDetails;

    protected AbstractProvider() {
        mLogTag = getClass().getName();
        mMatcherSynchronizer = new Object();
    }

    @Override
    public boolean onCreate() {
        try {
            SimpleSQLHelper dbHelper = new SimpleSQLHelper(getContext(), getDatabaseFileName(),
                    getSchemaVersion()) {

                @Override
                public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
                    super.onUpgrade(db, oldVersion, newVersion);

                    // Call onUpgrade of outer class so derived classes can extend the default
                    // behaviour
                    AbstractProvider.this.onUpgrade(db, oldVersion, newVersion);
                }

            };
            dbHelper.setTableClass(getClass());
            mDatabase = dbHelper.getWritableDatabase();
            return true;
        } catch (SQLiteException e) {
            Log.w(mLogTag, "Database Opening exception", e);
        }

        return false;
    }

    public static Uri makeUriFromSyncAdapter(Uri uri) {
        return uri.buildUpon().appendQueryParameter(QUERY_CALLER_IS_SYNC_ADAPTER, PARAM_TRUE).build();
    }

    /**
     * Initializes the matcher, based on the tables contained within the subclass,
     * also guarantees to have initialized the mTableNames[] array as well.
     */
    protected void initializeMatcher() {
        // initialize the UriMatcher once, use a synchronized block
        // here, so that subclasses can implement this by static initialization if
        // they want.
        synchronized (mMatcherSynchronizer) {
            if(mMatcher != null) {
                return;
            }

            mMatcher = new UriMatcher(UriMatcher.NO_MATCH);
            List<MatchDetail> details = new ArrayList<>();

            String authority = getAuthority();

            for (Class<?> clazz : getClass().getClasses()) {
                Table table = clazz.getAnnotation(Table.class);
                if (table != null) {
                    String tableName = Utils.getTableName(clazz, table);
                    String mimeName = Utils.getMimeName(clazz, table);

                    List<String> uniqueColumnsBuilder = new ArrayList<>();
                    List<String> foreignKeyUris = new ArrayList<>();
                    List<ForcedColumn> foreignKeyColumns = new ArrayList<>();

                    // Add the hierarchical versions
                    for (Field field : clazz.getFields()) {
                        Column column = field.getAnnotation(Column.class);
                        if(column == null) {
                            continue;
                        }

                        if(column.unique()) {
                            try {
                                uniqueColumnsBuilder.add(field.get(null).toString());
                            } catch (IllegalAccessException e) {
                                throw new IllegalArgumentException("Can't read the field name, needs to be static");
                            }
                        }

                        ForeignKey foreignKey = field.getAnnotation(ForeignKey.class);

                        if(foreignKey == null) {
                            continue;
                        }

                        Class<?> parent = foreignKey.references();

                        Table parentTable = parent.getAnnotation(Table.class);

                        if(parentTable == null) {
                            throw new IllegalArgumentException("Parent is not a table!");
                        }

                        String parentTableName = Utils.getTableName(parent, parentTable);

                        String foreignKeyColumn = null;

                        try {
                            foreignKeyColumn = field.get(null).toString();
                        } catch (IllegalAccessException e) {
                            throw new IllegalArgumentException("Can't read the field name, needs to be static");
                        }

                        String nestedUri = parentTableName + "/#/" + tableName;
                        foreignKeyUris.add(nestedUri);
                        foreignKeyColumns.add(new ForcedColumn(foreignKeyColumn, 1));
                    }

                    String[] uniqueColumns = uniqueColumnsBuilder.toArray(new String[uniqueColumnsBuilder.size()]);

                    // Add the plural version
                    mMatcher.addURI(authority, tableName, details.size());
                    details.add(new MatchDetail(tableName, "vnd.android.cursor.dir/vnd." + getAuthority() + "." + mimeName, null, uniqueColumns));

                    // Add the singular version
                    mMatcher.addURI(authority, tableName + "/#", details.size());
                    details.add(new MatchDetail(tableName, "vnd.android.cursor.item/vnd." + getAuthority() + "." + mimeName,  new ForcedColumn(BaseColumns._ID, 1), uniqueColumns));

                    for(int i = 0; i < foreignKeyUris.size(); i++) {
                        mMatcher.addURI(authority, foreignKeyUris.get(i), details.size());
                        details.add(new MatchDetail(tableName, "vnd.android.cursor.dir/vnd." + getAuthority() + "." + mimeName, foreignKeyColumns.get(i), uniqueColumns));
                    }
                }
            }

            // Populate the rest.
            mMatchDetails = details.toArray(new MatchDetail[details.size()]);
        }
    }

    private static class MatchDetail
    {
        public final String tableName;
        public final String mimeType;
        public final ForcedColumn forcedColumn;
        public final String[] uniqueColumns;

        public MatchDetail(String tableName, String mimeType, ForcedColumn forcedColumn, String[] uniqueColumns) {
            this.tableName = tableName;
            this.mimeType = mimeType;
            this.forcedColumn = forcedColumn;
            this.uniqueColumns = uniqueColumns;
        }
    }

    private static class ForcedColumn {
        public final String columnName;
        public final int pathSegment;

        private ForcedColumn(String columnName, int pathSegment) {
            this.columnName = columnName;
            this.pathSegment = pathSegment;
        }
    }

    /**
     * Called when the database needs to be updated and after <code>AbstractProvider</code> has
     * done its own work. That is, after creating columns that have been added using the
     * {@link Column#since()} key.<br>
     * <br>
     * For example: Let <code>AbstractProvider</code> automatically create new columns. Afterwards,
     * do more complicated work like calculating default values or dropping other columns inside
     * this method.<br>
     * <br>
     * This method executes within a transaction. If an exception is thrown, all changes will
     * automatically be rolled back.
     *
     * @param db         The database.
     * @param oldVersion The old database version.
     * @param newVersion The new database version.
     */
    protected void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        // override if needed
    }

    protected String getDatabaseFileName() {
        return getClass().getName().toLowerCase() + ".db";
    }

    /**
     * Returns the current schema version. This number will be used to automatically trigger
     * upgrades and downgrades. You may override this method in derived classes if anything has
     * changed in the schema classes.
     *
     * @return Current schema version.
     */
    protected int getSchemaVersion() {
        return 1;
    }

    protected abstract String getAuthority();

    @Override
    public String getType(Uri uri) {
        initializeMatcher();

        int match = mMatcher.match(uri);

        if(match == UriMatcher.NO_MATCH) {
            return null;
        }

        return mMatchDetails[match].mimeType;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        final SelectionBuilder builder = buildBaseQuery(uri);
        final Cursor cursor = builder.where(selection, selectionArgs).query(mDatabase, projection,
                sortOrder);
        if (cursor != null) {
            cursor.setNotificationUri(getContentResolver(), uri);
        }
        return cursor;
    }

    private ContentResolver getContentResolver() {
        Context context = getContext();
        if (context == null) {
            return null;
        }

        return context.getContentResolver();
    }

    private SelectionBuilder buildBaseQuery(Uri uri) {
        initializeMatcher();

        int match = mMatcher.match(uri);

        if(match == UriMatcher.NO_MATCH) {
            throw new IllegalArgumentException("Unsupported content uri");
        }

        MatchDetail detail = mMatchDetails[match];

        SelectionBuilder builder = new SelectionBuilder(detail.tableName);

        int conflictAlgorithm = SQLiteDatabase.CONFLICT_NONE;

        String serializedConflictAlgorithm = uri.getQueryParameter(QUERY_CONFLICT_ALGORITHM);

        if(serializedConflictAlgorithm != null) {
            conflictAlgorithm = Integer.parseInt(serializedConflictAlgorithm);
        }

        builder.setConflictAlgorithm(conflictAlgorithm);

        List<String> segments = uri.getPathSegments();
        if(detail.forcedColumn != null) {
            builder.whereEquals(detail.forcedColumn.columnName, segments.get(detail.forcedColumn.pathSegment));
        }

        return builder;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        initializeMatcher();

        int match = mMatcher.match(uri);

        if(match == UriMatcher.NO_MATCH) {
            throw new IllegalArgumentException("Unsupported content uri");
        }

        MatchDetail detail = mMatchDetails[match];

        if(detail.forcedColumn != null) {
            // most likely a foreign key, so let's add it to the values.
            values.put(detail.forcedColumn.columnName, Long.parseLong(uri.getPathSegments().get(detail.forcedColumn.pathSegment)));
        }

        int conflictAlgorithm = SQLiteDatabase.CONFLICT_NONE;

        String serializedConflictAlgorithm = uri.getQueryParameter(QUERY_CONFLICT_ALGORITHM);

        if(serializedConflictAlgorithm != null) {
            conflictAlgorithm = Integer.parseInt(serializedConflictAlgorithm);
        }

        // if we have ignore specified as a conflict strategy, attempt to insert the row,
        // but make it fail (without rollback) and in that case search for the correct value.
        // This is all to work around: https://code.google.com/p/android/issues/detail?id=13045
        // and here: http://stackoverflow.com/questions/13391915/why-does-insertwithonconflict-conflict-ignore-return-1-error

        long rowId = -1;

        try {
            rowId = mDatabase.insertWithOnConflict(detail.tableName, null, values, conflictAlgorithm == SQLiteDatabase.CONFLICT_IGNORE
                    ? SQLiteDatabase.CONFLICT_ABORT
                    : conflictAlgorithm);
        } catch(SQLiteConstraintException sce) {
            if(conflictAlgorithm == SQLiteDatabase.CONFLICT_NONE) {
                throw sce;
            }
        }

        if (rowId > -1) {
            boolean syncToNetwork = !PARAM_TRUE.equals(uri.getQueryParameter(QUERY_CALLER_IS_SYNC_ADAPTER));
            Uri canonical = Uri.parse("content://" + getAuthority() + "/" + detail.tableName);

            getContentResolver().notifyChange(uri, null, syncToNetwork);
            // when these differ it means there are two logical collections that the
            // content observers may be interested in.
            if(!canonical.equals(uri)) {
                getContentResolver().notifyChange(canonical, null, syncToNetwork);
            }

            return ContentUris.withAppendedId(canonical, rowId);
        } else if(conflictAlgorithm == SQLiteDatabase.CONFLICT_IGNORE) {
            // see: https://code.google.com/p/android/issues/detail?id=13045

            SelectionBuilder builder = new SelectionBuilder(detail.tableName);

            if(detail.forcedColumn != null) {
                builder.whereEquals(detail.forcedColumn.columnName, uri.getPathSegments().get(detail.forcedColumn.pathSegment));
            }

            for(int i = 0; i < detail.uniqueColumns.length; i++) {
                if(values.containsKey(detail.uniqueColumns[i])) {
                    builder.whereEquals(detail.uniqueColumns[i], values.getAsString(detail.uniqueColumns[i]));
                }
            }

            Cursor result = null;
            try {
                result = builder.query(mDatabase, new String[] {BaseColumns._ID}, null);
                if(!result.moveToFirst()) {
                    return null;
                }

                if(result.getCount() != 1) {
                    return null;
                }

                int idColumnIndex = result.getColumnIndex(BaseColumns._ID);

                rowId = result.getLong(idColumnIndex);
                Uri canonical = Uri.parse("content://" + getAuthority() + "/" + detail.tableName);
                return ContentUris.withAppendedId(canonical, rowId);
            } finally {
                if(result != null) {
                    result.close();
                }
            }
            // ensure that the match detail has the list of unique columns for that table
            // convert the list of values provided to just those with unique constraints
            // then select the row id from the table with those constraints.
        }

        return null;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final SelectionBuilder builder = buildBaseQuery(uri);
        int count = builder.where(selection, selectionArgs).delete(mDatabase);

        if (count > 0) {
            getContentResolver().notifyChange(uri, null, !PARAM_TRUE.equals(uri.getQueryParameter(QUERY_CALLER_IS_SYNC_ADAPTER)));
        }

        return count;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        final SelectionBuilder builder = buildBaseQuery(uri);
        int count = builder.where(selection, selectionArgs).update(mDatabase, values);

        if (count > 0) {
            getContentResolver().notifyChange(uri, null, !PARAM_TRUE.equals(uri.getQueryParameter(QUERY_CALLER_IS_SYNC_ADAPTER)));
        }

        return count;

    }

    @SuppressWarnings("NullableProblems")
    @Override
    public final ContentProviderResult[] applyBatch(ArrayList<ContentProviderOperation> operations)
            throws OperationApplicationException {
        ContentProviderResult[] result = null;
        mDatabase.beginTransaction();
        try {
            result = super.applyBatch(operations);
            mDatabase.setTransactionSuccessful();
        } finally {
            mDatabase.endTransaction();
        }
        return result;
    }

}
