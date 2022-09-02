package database

import (
	"context"
	"sort"
	"strings"
)

type DBCacheGenerator struct {
	repo DBRepository
}

func NewDBCacheUpdater(repo DBRepository) *DBCacheGenerator {
	return &DBCacheGenerator{
		repo: repo,
	}
}

func (u *DBCacheGenerator) GenerateDBCachePrimary(ctx context.Context) (*DBCache, error) {
	var err error
	dbCache := &DBCache{}
	dbCache.defaultSchema, err = u.repo.CurrentSchema(ctx)
	if err != nil {
		return nil, err
	}
	dbCache.Schemas, err = u.genSchemaCache(ctx)
	if err != nil {
		return nil, err
	}
	dbCache.SchemaTables, err = u.repo.SchemaTables(ctx)
	if err != nil {
		return nil, err
	}
	dbCache.ColumnsWithParent, err = u.genColumnCacheCurrent(ctx, dbCache.defaultSchema)
	if err != nil {
		return nil, err
	}
	dbCache.IndexWithParent, err = u.genIndexCacheCurrent(ctx, dbCache.defaultSchema)
	if err != nil {
		return nil, err
	}
	return dbCache, nil
}

func (u *DBCacheGenerator) GenerateDBCacheSecondary(ctx context.Context) (map[string][]*ColumnDesc, error) {
	return u.genColumnCacheAll(ctx)
}

func (u *DBCacheGenerator) genSchemaCache(ctx context.Context) (map[string]string, error) {
	dbs, err := u.repo.Schemas(ctx)
	if err != nil {
		return nil, err
	}
	databaseMap := map[string]string{}
	for _, db := range dbs {
		databaseMap[strings.ToUpper(db)] = db
	}
	return databaseMap, nil
}

func (u *DBCacheGenerator) genColumnCacheCurrent(ctx context.Context, schemaName string) (map[string][]*ColumnDesc, error) {
	columnDescs, err := u.repo.DescribeDatabaseTableBySchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}
	return genColumnMap(columnDescs), nil
}

func (u *DBCacheGenerator) genColumnCacheAll(ctx context.Context) (map[string][]*ColumnDesc, error) {
	columnDescs, err := u.repo.DescribeDatabaseTable(ctx)
	if err != nil {
		return nil, err
	}
	return genColumnMap(columnDescs), nil
}

func genColumnMap(columnDescs []*ColumnDesc) map[string][]*ColumnDesc {
	columnMap := map[string][]*ColumnDesc{}
	for _, desc := range columnDescs {
		key := desc.Schema + "\t" + desc.Table
		if _, ok := columnMap[key]; ok {
			columnMap[key] = append(columnMap[key], desc)
		} else {
			arr := []*ColumnDesc{desc}
			columnMap[key] = arr
		}
	}
	return columnMap

}

func (u *DBCacheGenerator) genIndexCacheCurrent(ctx context.Context, schemaName string) (map[string][]*IndexDesc, error) {
	indexDescs, err := u.repo.DescribeDatabaseIndexBySchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}
	return genIndexMap(indexDescs), nil
}

func genIndexMap(indexDescs []*IndexDesc) map[string][]*IndexDesc {
	indexMap := map[string][]*IndexDesc{}
	for _, desc := range indexDescs {
		key := desc.Schema + "\t" + desc.Table
		if _, ok := indexMap[key]; ok {
			indexMap[key] = append(indexMap[key], desc)
		} else {
			arr := []*IndexDesc{desc}
			indexMap[key] = arr
		}
	}
	return indexMap
}

type DBCache struct {
	defaultSchema     string
	Schemas           map[string]string
	SchemaTables      map[string][]string
	ColumnsWithParent map[string][]*ColumnDesc
	IndexWithParent   map[string][]*IndexDesc
}

func (dc *DBCache) Database(dbName string) (db string, ok bool) {
	db, ok = dc.Schemas[strings.ToUpper(dbName)]
	return
}

func (dc *DBCache) SortedSchemas() []string {
	dbs := []string{}
	for _, db := range dc.Schemas {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)
	return dbs
}

func (dc *DBCache) SortedTablesByDBName(dbName string) (tbls []string, ok bool) {
	tbls, ok = dc.SchemaTables[dbName]
	sort.Strings(tbls)
	return
}

func (dc *DBCache) SortedTables() []string {
	tbls, _ := dc.SortedTablesByDBName(dc.defaultSchema)
	return tbls
}

func (dc *DBCache) ColumnDescs(tableName string) (cols []*ColumnDesc, ok bool) {
	cols, ok = dc.ColumnsWithParent[columnDatabaseKey(dc.defaultSchema, tableName)]
	return
}

func (dc *DBCache) ColumnDatabase(dbName, tableName string) (cols []*ColumnDesc, ok bool) {
	cols, ok = dc.ColumnsWithParent[columnDatabaseKey(dbName, tableName)]
	return
}

func (dc *DBCache) Column(tableName, colName string) (*ColumnDesc, bool) {
	cols, ok := dc.ColumnsWithParent[columnDatabaseKey(dc.defaultSchema, tableName)]
	if !ok {
		return nil, false
	}
	for _, col := range cols {
		if strings.EqualFold(col.Name, colName) {
			return col, true
		}
	}
	return nil, false
}

func columnDatabaseKey(dbName, tableName string) string {
	return dbName + "\t" + tableName
}

func (dc *DBCache) IndexDescs(tableName string) (idx []*IndexDesc, ok bool) {
	idx, ok = dc.IndexWithParent[indexDatabaseKey(dc.defaultSchema, tableName)]
	return
}

func indexDatabaseKey(dbName, tableName string) string {
	return dbName + "\t" + tableName
}
