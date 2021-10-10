package main

import (
	"fmt"
	"os"
	"sync"
	"encoding/json"
	"path/filepath"
	"io/ioutil"
	"github.com/jcelliott/lumber"
)

const Version = "1.0.0"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}

	Driver struct {
		mutex sync.Mutex
		mutexes map[string] *sync.Mutex
		dir string
		log Logger
	}
)

type Options struct {
	Logger
}

func New(dir string, options *Options)(*Driver, error) {
	dir = filepath.Clean(dir)

	opts := Options{}

	if options != nil {
		opts = *options
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger((lumber.info))
	}

	driver := Driver {
		dir: dir,
		mutexes: make(map[string] *sync.Mutex),
		log: opts.Logger,
	}

	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("Using '%s' (database already exists)\n", dir)
		return &driver, nil
	}

	opts.Logger.Debug("Creating the database at '%s'...\n", dir)

	return &driver, os.MkdirAll(dir, 0755)
}

// struct methods
func (d *Driver) Write(collectionName, resource string, v interface{}) error {
	if collectionName == "" {
		return fmt.Errorf("Missing collectionName - no place to save record!")
	}

	if resource == "" {
		return fmt.Errorf("Missing collectionName - unable to save record (no name)!")
	}

	mutex := d.getOrCreateMutex(collectionName)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collectionName)

	fnlPath := filepath.Join(dir, resource + ".json")
	tmpPath := fnlPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	b, err = json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	b = append(b, byte('\n'))

	if err := ioutil.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, fnlPath)
}

func (d *Driver) Read(collectionName, resource string, v interface{}) error {
	if collectionName == "" {
		return fmt.Errorf("Missing collectionName - unable to read record!")
	} 

	if resource == "" {
		return fmt.Errorf("Missing collectionName - unable to read record (no name)!")
	}

	record := filepath.Join(d.dir, collectionName, resource)

	if _, err := stat(record); err !=nil {
		return err
	}

	b, err := ioutil.ReadFile(record + ".json")

	if err != nil {
		return err
	}

	return json.Unmarshal(b, &v)
}

func (d *Driver) ReadAll(collectionName string)([]string, error) {
	if collectionName == "" {
		return fmt.Errorf("Missing collectionName - unable to read records!")
	} 
	
	dir := filepath.Join(d.dir, collectionName)

	if _, err := stat(dir); err != nil {
		return nil, err
	}

	files, _ := ioutil.ReadDir(dir)

	var records []string
	
	for _, file := range files {
		b, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil{
			return nil, err
		}

		records = append(records, string(b))
	}

	return records, nil
}

func (d *Driver) Delete(collectionName, resource string) error {
	path := filepath.Join(collectionName, resource)
	
	mutex := d.getOrCreateMutex(collectionName)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)

	switch fi, err := stat(dir); {
	case fi == nil, err != nil:
		return fmt.Errorf("Unable to find file or directory named %v\n", path)

	case fi.Mode().IsDir():
		return os.RemoveAll(dir)
		
	case fi.Mode().isRegular():
		return os.RemoveAll(dir + ".json")
	}
}

func (d *Driver) getOrCreateMutex(collectionName string) *sync.Mutex {

	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collectionName]

	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collectionName] = m
	}

	return m
}

func stat(path string)(fi os.FileInfo, err error) {
	if fi, err = os.Stats(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}

	return 
}

type Address struct {
	City string
	State string
	Country string
	Pincode json.Number
}

type User struct {
	Name string
	Age json.number
	Contact string
	Company string
	Address Address
}

func main() {
	dir := "./"

	db, err := New(dir, null)
	if err != nil {
		fmt.Println("Error", err)
	}

	employees := []User {
		{"John", "25", "234235235", "Google", Address {"New-York", "NY", "USA", "123123"}},
		{"Mike", "25", "234235235", "Facebook", Address {"Chicago", "Illinois", "USA", "123123"}},
		{"Sam", "25", "234235235", "Microsoft", Address {"Chicago", "Illinois", "USA", "123123"}},
		{"Oliver", "25", "234235235", "IBM", Address {"Chicago", "Illinois", "USA", "123123"}},
		{"Zack", "25", "234235235", "Netflix", Address {"Chicago", "Illinois", "USA", "123123"}},
		{"Tyler", "25", "234235235", "Amazon", Address {"Chicago", "Illinois", "USA", "123123"}},
		{"Simon", "25", "234235235", "Tesla", Address {"Chicago", "Illinois", "USA", "123123"}},
	}

	for _,dto := range employees {
		db.Write("users", dto.Name, User{
			Name: dto.Name,
			Age: dto.Age,
			Contact: dto.Contact,
			Company: dto.Company,
			Address: dto.Address,
		})
	}

	records, err := db.ReadAll("users")

	if err != nil {
		fmt.Println("Error", err)
	}

	fmt.Println(records)

	allUsers := []User{}

	for _, f = range records {
		employeeFound := User{}
		if err := json.Unmarshal([]byte(f), &employeeFound); err != nil {
			fmt.Println("Error", err)
		}
		allUsers = append(allUsers, employeeFound)
	}

	fmt.Println((allUsers))

	// if err := db.Delete("users", "John"); err != nil {
	// 	fmt.Println("Error", err)
	// }

	// if err := db.Delete("users", ""); err != nil {
	// 	fmt.Println("Error", err)
	// }
}