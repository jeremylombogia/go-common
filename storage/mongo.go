package storage

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"

	"github.com/sahalazain/go-common/config"
	"github.com/sahalazain/go-common/logger"
	"go.mongodb.org/mongo-driver/mongo"
	"gocloud.dev/docstore"
)

func InitMongoDB(ctx context.Context, conf config.Getter) error {
	log := logger.GetLoggerContext(ctx, "storage", "InitMongoDB")
	log.Debug("initialize mongo DB")
	var tmp map[string]interface{}
	if err := conf.Unmarshal(&tmp); err != nil {
		log.WithError(err).Error()
		return err
	}

	ms, out := findMongoDef(tmp)
	if ms == "" {
		return errors.New("[CachedCollection] mongo server URL not found")
	}

	if len(out) == 0 {
		return errors.New("[CachedCollection] mongo collection config not found")
	}

	os.Setenv("MONGO_SERVER_URL", ms)

	col, err := docstore.OpenCollection(ctx, out[0])
	if err != nil {
		log.WithError(err).Error("[CachedCollection] Error opening collection")
		return err
	}
	defer col.Close()
	var mcol *mongo.Collection
	var db *mongo.Database
	if col.As(&mcol) {
		db = mcol.Database()
	}

	if db == nil {
		log.Error("error get database connection")
		return errors.New("[CachedCollection] cannot get database connection")
	}

	log.Debug("creating mongo collection")
	for _, c := range out {
		u, err := url.Parse(c)
		if err != nil {
			log.WithField("url", c).WithError(err).Warn("Error parsing collection URL")
			continue
		}
		cn := strings.Trim(u.Path, "/")
		if err := db.CreateCollection(ctx, cn); err != nil {
			log.WithField("name", cn).WithError(err).Warn("Error creating collection")
		}
	}

	return nil
}

func findMongoDef(conf map[string]interface{}) (string, []string) {
	ms := ""
	out := make([]string, 0)
	for k, v := range conf {
		switch v := v.(type) {
		case string:
			if k == "mongo_server_url" {
				ms = v
				continue
			}
			if strings.HasPrefix(v, "mongo://") {
				out = append(out, v)
			}
		case map[string]string:
			for _, w := range v {
				if strings.HasPrefix(w, "mongo://") {
					out = append(out, w)
				}
			}
		case map[string]interface{}:
			_, o := findMongoDef(v)
			if len(o) > 0 {
				out = append(out, o...)
			}
		default:
			continue
		}

	}
	return ms, out
}
