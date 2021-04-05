package storage

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gocloud.dev/docstore"
)

//QueryOpt query option
type QueryOpt struct {
	Limit    int
	Skip     int
	OrderBy  string
	IsAscend bool
	Filter   []FilterOpt
}

func (q *QueryOpt) AddFilter(filter FilterOpt) *QueryOpt {
	for _, f := range q.Filter {
		if f.Field == filter.Field {
			return q
		}
	}

	q.Filter = append(q.Filter, filter)
	return q
}

func (q *QueryOpt) ToMongoFilter() (bson.M, *options.FindOptions) {
	d := bson.M{}
	for _, s := range q.Filter {
		d[s.Field] = s.ToMongoM()
	}

	opt := options.Find()
	opt.SetLimit(int64(q.Limit))
	opt.SetSkip(int64(q.Skip))
	if q.OrderBy != "" {
		dir := -1
		if q.IsAscend {
			dir = 1
		}
		opt.SetSort(bson.D{{Key: q.OrderBy, Value: dir}})
	}

	return d, opt
}

func (q *QueryOpt) ToDocstoreQuery(query *docstore.Query) {
	if q.Filter != nil && len(q.Filter) > 0 {
		for _, f := range q.Filter {
			if f.Field == "id" {
				f.Field = "_id"
			}
			query.Where(docstore.FieldPath(f.Field), f.Ops, f.Value)
		}
	}

	if q.Limit > 0 {
		query.Limit(q.Limit)
	}

	if q.OrderBy != "" {
		d := docstore.Descending
		if q.IsAscend {
			d = docstore.Ascending
		}
		query.OrderBy(q.OrderBy, d)
	}

}

func (q *QueryOpt) Normalize() *QueryOpt {
	fs := make([]FilterOpt, 0)
	for _, f := range q.Filter {
		switch f.Ops {
		case "!=":
			continue
		case "in", "inmatch", "match":
			f.Ops = "="
		}
		fs = append(fs, f)
	}
	q.Filter = fs
	return q
}

//FilterOpt filter option
type FilterOpt struct {
	Field string
	Value interface{}
	Ops   string
}

func (f *FilterOpt) ToMongoM() bson.M {
	switch f.Ops {
	case "=":
		return bson.M{
			"$eq": f.Value,
		}
	case "<":
		return bson.M{
			"$lt": f.Value,
		}
	case "<=":
		return bson.M{
			"$lte": f.Value,
		}
	case ">":
		return bson.M{
			"$gt": f.Value,
		}
	case ">=":
		return bson.M{
			"$gte": f.Value,
		}
	case "!=":
		return bson.M{
			"$ne": f.Value,
		}
	case "in":
		return bson.M{
			"$in": f.Value,
		}
	case "inmatch":
		return bson.M{
			"$elemMatch": f.Value,
		}
	case "match":
		return bson.M{
			"$regex": primitive.Regex{
				Pattern: fmt.Sprintf("%v", f.Value),
				Options: "i",
			},
		}
	default:
		return nil
	}
}
