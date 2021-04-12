package storage

import (
	"context"
	"errors"
	"io"
	"net/url"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/sahalazain/go-common/config"
	"github.com/sahalazain/simplecache"
	"go.mongodb.org/mongo-driver/mongo"
	"gocloud.dev/docstore"
	"gocloud.dev/gcerrors"
)

const defaultExpiration = 3600 * 24

type Document interface {
	GetID() string
	GenerateID() Document
	SetCreatedTime(t time.Time) Document
	GetCreatedTime() time.Time
}

type CachedCollection struct {
	CollectionURL     string `json:"collection_url,omitempty" mapstructure:"collection_url"`
	CacheURL          string `json:"cache_url,omitempty" mapstructure:"cache_url"`
	CacheExpiration   int    `json:"cache_expiration,omitempty" mapstructure:"cache_expiration"`
	EnableTransaction bool   `json:"enable_transaction,omitempty" mapstructure:"enable_transaction"`
	Driver            string
	Collection        *docstore.Collection
	Cache             simplecache.Cache
}

func New(ctx context.Context, conf config.Getter) (*CachedCollection, error) {
	var cc CachedCollection
	if err := conf.Unmarshal(&cc); err != nil {
		return nil, err
	}
	if err := cc.init(ctx); err != nil {
		return nil, err
	}
	return &cc, nil
}

func (c *CachedCollection) init(ctx context.Context) error {

	if c.CacheURL == "" {
		return errors.New("[CachedCollection] missing cache_url param")
	}

	if c.CollectionURL == "" {
		return errors.New("[CachedCollection] missing collection_url param")
	}

	cache, err := simplecache.New(c.CacheURL)
	if err != nil {
		return err
	}
	c.Cache = cache

	u, err := url.Parse(c.CollectionURL)
	if err != nil {
		return err
	}

	c.Driver = u.Scheme

	col, err := docstore.OpenCollection(ctx, c.CollectionURL)
	if err != nil {
		return err
	}
	c.Collection = col

	if c.CacheExpiration == 0 {
		c.CacheExpiration = defaultExpiration
	}

	return nil
}

func (c *CachedCollection) Create(ctx context.Context, doc Document) error {
	return c.Collection.Create(ctx, doc.SetCreatedTime(time.Now()).GenerateID())
}

func (c *CachedCollection) BulkCreate(ctx context.Context, docs []Document) error {
	if len(docs) == 0 {
		return errors.New("[CachedCollection] empty document list")
	}
	acl := c.Collection.Actions()
	for _, d := range docs {
		acl.Create(d.SetCreatedTime(time.Now()).GenerateID())
	}
	return acl.Do(ctx)
}

func (c *CachedCollection) BulkGet(ctx context.Context, out []interface{}) error {
	if len(out) == 0 {
		return errors.New("[CachedCollection] empty documents placeholder")
	}

	act := c.Collection.Actions()

	for _, d := range out {
		act = act.Get(d)
	}

	if err := act.Do(ctx); err != nil {
		return err
	}

	return nil
}

func (c *CachedCollection) Get(ctx context.Context, out Document) error {
	if out.GetID() == "" {
		return errors.New("[CachedCollection] missing id value")
	}

	if c.Cache.Exist(ctx, out.GetID()) {
		if err := c.Cache.GetObject(ctx, out.GetID(), out); err == nil {
			return nil
		}
	}

	if err := c.Collection.Get(ctx, out); err != nil {
		code := gcerrors.Code(err)
		if code != gcerrors.NotFound {
			return err
		}
		out = nil
		return nil
	}

	if err := c.Cache.Set(ctx, out.GetID(), out, c.CacheExpiration); err != nil {
		return err
	}
	return nil
}

func (c *CachedCollection) UpdateField(ctx context.Context, doc Document, fieldName string, value interface{}) error {
	if doc.GetID() == "" {
		return errors.New("[CachedCollection] missing id value")
	}

	if err := c.Cache.Delete(ctx, doc.GetID()); err != nil {
		return err
	}

	return c.Collection.Update(ctx, doc, docstore.Mods{docstore.FieldPath(fieldName): value})
}

func (c *CachedCollection) Increment(ctx context.Context, doc Document, fieldName string, value int) error {
	if doc.GetID() == "" {
		return errors.New("[CachedCollection] missing id value")
	}

	if err := c.Cache.Delete(ctx, doc.GetID()); err != nil {
		return err
	}

	return c.Collection.Update(ctx, doc, docstore.Mods{docstore.FieldPath(fieldName): docstore.Increment(value)})
}

func (c *CachedCollection) Update(ctx context.Context, doc Document) error {
	if doc.GetID() == "" {
		return errors.New("[CachedCollection] missing id value")
	}

	if err := c.Cache.Delete(ctx, doc.GetID()); err != nil {
		return err
	}
	return c.Collection.Put(ctx, doc)
}

func (c *CachedCollection) Replace(ctx context.Context, doc Document) error {
	if doc.GetID() == "" {
		return errors.New("[CachedCollection] missing id value")
	}

	if err := c.Cache.Delete(ctx, doc.GetID()); err != nil {
		return err
	}
	return c.Collection.Replace(ctx, doc)
}

func (c *CachedCollection) Delete(ctx context.Context, doc Document) error {
	if doc.GetID() == "" {
		return errors.New("[CachedCollection] missing id value")
	}

	if err := c.Cache.Delete(ctx, doc.GetID()); err != nil {
		return err
	}
	return c.Collection.Delete(ctx, doc)
}

func (c *CachedCollection) Find(ctx context.Context, opt *QueryOpt, out interface{}) error {
	if opt == nil {
		return errors.New("missing query option")
	}
	if c.Driver == "mongo" {
		return c.mongoFind(ctx, opt, out)
	}
	return c.dsFind(ctx, opt, out)
}

func (c *CachedCollection) dsFind(ctx context.Context, opt *QueryOpt, out interface{}) error {
	q := c.Collection.Query()
	opt.Normalize().ToDocstoreQuery(q)

	iter := q.Get(ctx)
	defer iter.Stop()

	skip := opt.Skip

	docs := make([]map[string]interface{}, 0)
	for {
		d := make(map[string]interface{})
		err := iter.Next(ctx, d)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if skip > 0 {
			skip--
			continue
		}

		docs = append(docs, d)
	}

	if err := mapstructure.Decode(docs, out); err != nil {
		return err
	}

	return nil
}

func (c *CachedCollection) mongoFind(ctx context.Context, opt *QueryOpt, out interface{}) error {
	if c.Driver != "mongo" {
		return errors.New("[CachedCollection] this function only for mongo driver")
	}
	var col *mongo.Collection
	if c.Collection.As(&col) {
		query, fopt := opt.ToMongoFilter()

		cur, err := col.Find(ctx, query, fopt)
		if err != nil {
			return err
		}

		if err := cur.All(ctx, out); err != nil {
			return err
		}

	}
	return nil
}

//GetMongoSession get mongo session
func (c *CachedCollection) GetMongoSession() (mongo.Session, error) {
	if c.Driver != "mongo" || !c.EnableTransaction {
		return nil, nil
	}
	var col *mongo.Collection
	if c.Collection.As(&col) {
		client := col.Database().Client()
		return client.StartSession()
	}
	return nil, nil
}
