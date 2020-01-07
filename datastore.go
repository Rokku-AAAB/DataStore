package datastore

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"gitlab.atvg-studios.com/atvg-studios/go-database/v2/database"
	"io"
	"io/ioutil"
	"os"
	"time"
)

type DataStore struct {
	db *database.Database
}

// User struct
type User struct {
	UUID      string `json:"uuid,omitempty" bson:"uuid,omitempty"`
	Name      string `json:"name,omitempty" bson:"name,omitempty"`
	TimeOfBan int64  `json:"timeofban,omitempty" bson:"timeofban,omitempty"`
}

// Users type
type Users []User

func New(path string) (DataStore, error) {
	db, err := database.New(database.USE_BITCASK, path)
	return DataStore{db: &db}, err
}

func NewWithDatabase(db *database.Database) DataStore {
	return DataStore{db: db}
}

func NewEmpty() (DataStore, error) {
	db, err := database.New(database.USE_BITCASK, os.TempDir())
	return DataStore{db: &db}, err
}

func (ds DataStore) Put(user User) error {
	uuid := bytes.NewBufferString(user.UUID).Bytes()

	if has, err := ds.db.Has(uuid); !has && err != nil {
		buffer, err1 := Encode(user)

		if err1 != nil {
			return err1
		}

		_, err2 := ds.db.Put(uuid, buffer)

		if err2 != nil {
			return err2
		}
	}

	return nil
}

func (ds DataStore) Get(uuid string) (User, error) {
	uuidBytes := bytes.NewBufferString(uuid).Bytes()

	if has, err := ds.db.Has(uuidBytes); has && err == nil {
		buffer, err1 := ds.db.Get(uuidBytes)

		if err1 != nil {
			return User{}, err1
		}

		user, err2 := Decode(buffer)

		if err2 != nil {
			return User{}, err2
		}

		return user, nil
	}

	return User{}, errors.New("cannot find key")
}

func (ds DataStore) Delete(uuid string) error {
	uuidBytes := bytes.NewBufferString(uuid).Bytes()

	return ds.db.Delete(uuidBytes)
}

func (ds DataStore) All() (Users, error) {
	buffer, err := ds.db.All()

	if err != nil {
		return Users{}, err
	}

	var users Users
	var user User
	var err1 error

	for _, buf := range buffer {
		user, err1 = Decode(buf)

		if err1 != nil {
			return Users{}, err1
		}

		users = append(users, user)
	}

	return users, nil
}

func (ds DataStore) Import(filename string, outstream io.Writer) error {
	file, err1 := os.Open(filename)
	if !isnil(err1) {
		return err1
	}

	byteValue, err2 := ioutil.ReadAll(file)
	if !isnil(err2) {
		return err2
	}

	var users Users

	err3 := json.Unmarshal(byteValue, &users)
	if !isnil(err3) {
		return err3
	}

	_, _ = fmt.Fprintf(outstream, "Importing %d items into BitCask", len(users))

	var importCount int
	var ignoreCount int

	for _, user := range users {
		if user.UUID == "" {
			return errors.New("cannot import User with Empty UUID")
		}

		if user.TimeOfBan == 0 {
			user.TimeOfBan = time.Now().Unix()
		}

		err4 := ds.Put(user)

		if err4 != nil {
			ignoreCount++
		} else {
			importCount++
		}
	}

	_, _ = fmt.Fprintf(outstream, "Result: Imported %d, Ignored %d", importCount, ignoreCount)

	return nil
}

func isnil(err error) bool {
	return err == nil
}

func Encode(user User) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)

	err := enc.Encode(user)

	if err != nil {
		return []byte{}, err
	}

	return buffer.Bytes(), nil
}

func Decode(byts []byte) (User, error) {
	var buffer bytes.Buffer
	_, err1 := buffer.Write(byts)

	if err1 != nil {
		return User{}, err1
	}

	dec := gob.NewDecoder(&buffer)

	var user User
	err := dec.Decode(&user)

	if err != nil {
		return User{}, err
	}

	return user, nil
}