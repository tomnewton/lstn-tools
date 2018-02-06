# Lstn.in db seeder

Add podcasts to the db from an RSS feed. 

## Getting started

### System Dependencies

You'll need to have the go 1.9.3+ sdk installed. 


### Install the firebase package.

```bash
cd projdir
go get ./...
```

## Usage

```bash
go run main.go
```

Then enter the feed url you want to insert into the db. 

Note you can also use: "delete" to completely delete all data in firestore, or "rebuild" to delete and rebuild from all static feeds in main.go.