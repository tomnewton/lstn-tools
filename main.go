package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/mmcdole/gofeed"
	"github.com/spf13/viper"

	firebase "firebase.google.com/go"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var app *firebase.App

func podcastExistsInDB(ctx context.Context, client *firestore.Client, podcast *Podcast) (bool, error) {
	doc, err := client.Collection("podcasts").Doc(podcast.ID).Get(ctx)

	if err != nil {
		if grpc.Code(err) == codes.NotFound {
			return false, nil
		}
		fmt.Println(fmt.Errorf("error checking if podcast exists: %s", err))
		return false, err
	}

	if doc != nil {
		return true, nil
	}

	return false, nil
}

func md5hash(input string) string {
	h := md5.New()
	io.WriteString(h, input)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// FeedResult ...
type FeedResult struct {
	Podcast  *Podcast
	Episodes []*Episode
}

//Podcast - summaries a podcast
type Podcast struct {
	Title          string
	Author         *Author
	Description    string
	Link           string
	ID             string
	ImageOriginal  *Image
	ImageThumbnail *Image
	Language       string
	Explicit       string

	Categories *[]string
	Copyright  string
}

//NewPodcast - creates new Podcast
func NewPodcast(feed *gofeed.Feed) *Podcast {

	_, err := url.Parse(feed.Link)

	_, err = url.Parse(feed.Image.URL)

	if err != nil {
		panic(fmt.Errorf("%s", err))
	}

	return &Podcast{
		Title:          feed.Title,
		Author:         &Author{Name: feed.Author.Name, Email: feed.Author.Email},
		Description:    feed.Description,
		Link:           feed.Link,
		ID:             md5hash(feed.Link),
		ImageOriginal:  &Image{Title: feed.Image.Title, URL: feed.Image.URL},
		ImageThumbnail: &Image{},
		Language:       feed.Language,
		Explicit:       feed.ITunesExt.Explicit,
		Categories:     &feed.Categories,
		Copyright:      feed.Copyright}
}

//Author - podcast author
type Author struct {
	Name  string
	Email string
}

//Image ...
type Image struct {
	Title string
	URL   string
}

//NewEpisode - creates new Episode
func NewEpisode(item *gofeed.Item) *Episode {

	_, err := url.Parse(item.Image.URL)
	if err != nil {
		panic(fmt.Errorf("fatal: %s", err))
	}

	enclosures := []*Enclosure{}

	for _, enclosure := range item.Enclosures {
		_, err := url.Parse(enclosure.URL)

		var mediaLength int64

		if enclosure.Length != "" {
			mediaLength, err = strconv.ParseInt(enclosure.Length, 10, 64)
		}

		if err != nil {
			fmt.Println(enclosure)
			panic(fmt.Errorf("fatal: %s", err))
		}

		e := &Enclosure{
			URL:    enclosure.URL,
			Type:   enclosure.Type,
			Length: mediaLength,
		}

		enclosures = append(enclosures, e)
	}

	return &Episode{
		ID:          md5hash(item.GUID),
		Title:       item.Title,
		Published:   item.PublishedParsed,
		Author:      &Author{Name: item.Author.Name, Email: item.Author.Email},
		Description: item.Description,
		Image:       &Image{Title: item.Image.Title, URL: item.Image.URL},
		Enclosures:  enclosures,
		GUID:        item.GUID,
		ITunesEpisodeExt: &ITunesEpisodeExt{
			Summary:  item.ITunesExt.Summary,
			Explicit: item.ITunesExt.Explicit,
			Duration: item.ITunesExt.Duration,
			Keywords: item.ITunesExt.Keywords,
		}}
}

//Episode - an episode of a podcast
type Episode struct {
	ID               string
	Title            string
	Published        *time.Time
	Author           *Author
	Description      string
	Image            *Image
	Enclosures       []*Enclosure
	GUID             string
	ITunesEpisodeExt *ITunesEpisodeExt
}

//Enclosure - funny name but that is what
//they call the actual media in the feed.
type Enclosure struct {
	URL    string
	Type   string
	Length int64
}

//ITunesEpisodeExt - extra iTunes tags.
type ITunesEpisodeExt struct {
	Summary  string
	Explicit string
	Duration string
	Keywords string
}

func insert(ctx context.Context, client *firestore.Client, result *FeedResult) error {

	//check firestore to see if this podcast exists.
	exists, err := podcastExistsInDB(ctx, client, result.Podcast)
	if err != nil {
		return err
	}

	fmt.Printf("Podcast %s exists? %t\n", result.Podcast.Title, exists)

	//Create documentReference for the Podcast.
	docRef := client.Collection("podcasts").Doc(result.Podcast.ID)

	if !exists {
		//Create the Podcast document in podcasts collection.
		_, err := docRef.Set(ctx, result.Podcast)
		if err != nil {
			fmt.Println(fmt.Errorf("Couldn't create podcast document in db: %s", err))
		}
		fmt.Printf("Created new Podcast: %s by %s\n", result.Podcast.Title, result.Podcast.Author.Name)
	}

	//Batch write episodes
	batch := client.Batch()
	i := 0
	for _, ep := range result.Episodes {
		ref := client.Collection("podcasts").Doc(result.Podcast.ID).Collection("episodes").Doc(ep.ID)
		batch.Set(ref, ep)
		if i == 499 { //500 episodes is enough, and is the max Batch put.
			break
		}
	}

	fmt.Printf("Batch writing %d episodes...\n", len(result.Episodes))
	_, err = batch.Commit(ctx)
	if err != nil {
		return err
	}
	fmt.Println("done.")
	return err
}

// loadRSSFeed - loads a feed.
func loadRSSFeed(feedURL string) (*FeedResult, error) {
	feed, err := gofeed.NewParser().ParseURL(feedURL)

	if err != nil {
		return nil, err
	}

	//sometimes the email is blank, but is in the itunes:extensions.
	if feed.Author.Email == "" {
		feed.Author.Email = feed.ITunesExt.Owner.Email
	}

	r := FeedResult{}
	r.Podcast = NewPodcast(feed)
	r.Episodes = []*Episode{}

	for _, item := range feed.Items {
		if item.Author == nil || item.Author.Email == "" {
			item.Author = &gofeed.Person{}
			item.Author.Name = feed.Author.Name
			item.Author.Email = feed.Author.Email

		}

		if item.Image == nil {
			item.Image = feed.Image
		}

		r.Episodes = append(r.Episodes, NewEpisode(item))
	}

	//debug
	//data, _ := json.MarshalIndent(r, "", "	")
	//fmt.Printf("%s\n", data)

	return &r, nil
}

func prompt(ctx context.Context) *FeedResult {

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter Feed URL: ")
	text, _ := reader.ReadString('\n')

	result, err := loadRSSFeed(text)
	if err != nil {
		panic(fmt.Errorf("error: %s", err))
	}

	return result
}

func main() {

	viper.SetConfigFile("config.yaml")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	sa := option.WithCredentialsFile(
		filepath.Clean(viper.Get("service-account-path").(string)))

	ctx := context.Background()
	app, err := firebase.NewApp(ctx, nil, sa)
	if err != nil {
		log.Fatalln(err)
	}

	client, err := app.Firestore(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	defer client.Close()

	result := prompt(ctx)

	insert(ctx, client, result)
}
