package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	"image/png"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/mmcdole/gofeed"
	"github.com/nfnt/resize"
	"github.com/spf13/viper"

	"cloud.google.com/go/storage"
	firebase "firebase.google.com/go"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var app *firebase.App

var feeds = []string{
	"http://feeds.gimletmedia.com/crimetownshow",
	"http://feeds.gimletmedia.com/eltshow",
	"http://feeds.gimletmedia.com/heavyweightpodcast",
	"http://feeds.gimletmedia.com/hearstartup",
	"http://feeds.gimletmedia.com/sciencevs",
	"http://feeds.gimletmedia.com/hearreplyall",
	"http://feeds.gimletmedia.com/mogulshow",
	"http://feeds.gimletmedia.com/homecomingshow",
	"http://feeds.gimletmedia.com/storypirates",
	"http://feeds.gimletmedia.com/thenodshow",
	"http://feeds.gimletmedia.com/thepitchshow",
	"http://podcasts.files.bbci.co.uk/p05n1r2s.rss",                   // Radio1 & 1Xtra Stories
	"http://podcasts.files.bbci.co.uk/p05nrmhm.rss",                   // BBC Womans Hour
	"http://podcasts.files.bbci.co.uk/b006qptc.rss",                   // World at 1
	"http://podcasts.files.bbci.co.uk/b00snr0w.rss",                   // infinite monkey cage
	"http://podcasts.files.bbci.co.uk/b006qnx3.rss",                   // the food programme
	"https://www.npr.org/rss/podcast.php?id=510289",                   // planet money
	"https://www.npr.org/rss/podcast.php?id=510308",                   // hidden brain
	"http://feed.thisamericanlife.org/talpodcast",                     // this american life
	"http://www.espn.com/espnradio/feeds/rss/podcast.xml?id=10672984", //ESPN FC
	"http://www.espn.com/espnradio/feeds/rss/podcast.xml?id=2406595",  // Pardon The Interruption
	"http://www.espn.com/espnradio/feeds/rss/podcast.xml?id=18339885", // The Adam Schefter Podcast
	"http://www.espn.com/espnradio/feeds/rss/podcast.xml?id=2839445",  // Around the Horn
	"http://www.espn.com/espnradio/feeds/rss/podcast.xml?id=14805210", // Around the Rim
	"https://thefantasyfootballers.libsyn.com/fantasyfootball",        // The Fantasy Footballers
	"http://feeds.feedburner.com/freakonomicsradio",
	"http://feeds.wnyc.org/radiolab",
	"http://feeds.wnyc.org/wnycheresthething",
	"http://feeds.wnyc.org/newyorkerradiohour",
	"http://feeds.soundcloud.com/users/soundcloud:users:62921190/sounds.rss", // a16z podcast,
	"https://rss.simplecast.com/podcasts/3408/rss",                           // the kevin rose show
	"https://rss.simplecast.com/podcasts/4267/rss",                           // Block Zero
}

func deleteCollection(ctx context.Context, client *firestore.Client,
	ref *firestore.CollectionRef, batchSize int) error {

	for {
		// Get a batch of documents
		iter := ref.Limit(batchSize).Documents(ctx)
		numDeleted := 0

		// Iterate through the documents, adding
		// a delete operation for each one to a
		// WriteBatch.
		batch := client.Batch()
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}

			batch.Delete(doc.Ref)
			numDeleted++
		}

		// If there are no documents to delete,
		// the process is over.
		if numDeleted == 0 {
			return nil
		}

		_, err := batch.Commit(ctx)
		if err != nil {
			return err
		}
	}
}

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
	FeedLink       string
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

	if _, err := url.ParseRequestURI(feed.Link); err != nil {
		panic(fmt.Errorf("error: feed.Link : %s", err))
	}

	if _, err := url.ParseRequestURI(feed.Image.URL); err != nil {
		panic(fmt.Errorf("error: feed.Image.URL : %s", err))
	}

	if _, err := url.ParseRequestURI(feed.FeedLink); err != nil {
		panic(fmt.Errorf("error: feed.FeedLink : %s", err))
	}

	return &Podcast{
		Title:          feed.Title,
		Author:         &Author{Name: feed.Author.Name, Email: feed.Author.Email},
		Description:    feed.Description,
		FeedLink:       feed.FeedLink,
		Link:           feed.Link,
		ID:             md5hash(feed.FeedLink),
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
	Data  []byte `firestore:",omitempty"`
}

//NewEpisode - creates new Episode
func NewEpisode(item *gofeed.Item, podcastID string) *Episode {

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

	if len(enclosures) == 0 || !strings.HasPrefix( enclosures[0].URL, "http") {
		return nil
	}

	mediaURL, err := url.Parse(enclosures[0].URL)
	if err != nil {
		fmt.Errorf("issue with path of media enclosure: %s", err)
		return nil
	}

	//currently we only support these file types.
	if strings.HasSuffix(mediaURL.Path, ".mp3") == false &&
		strings.HasSuffix(mediaURL.Path, ".m4a") == false {
		return nil
	}

	return &Episode{
		PodcastID:   podcastID,
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
	PodcastID        string
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

func resizeImageFromURL(url string, size uint) (*image.Image, string, error) {
	imgReadCloser, err := loadImage(url)
	if err != nil {
		return nil, "", err
	}
	img, format, err := image.Decode(io.Reader(imgReadCloser))
	if err != nil {
		return nil, "", err
	}

	thumb := resize.Thumbnail(size, size, img, resize.NearestNeighbor)

	return &thumb, format, nil
}

func loadImage(url string) (io.ReadCloser, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
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
		//Create the thumbnail images we need for the Podcast,
		fmt.Println("Creating podcasts thumbnail image from ", result.Podcast.ImageOriginal.URL)
		img, _, err := resizeImageFromURL(result.Podcast.ImageOriginal.URL, uint(viper.Get("thumbnail-size").(int)))
		if err != nil {
			panic(fmt.Errorf("error resizing image %s", err))
		}

		//and stick them into CloudStorage.
		//projectID := viper.Get("google-cloud-project-id").(string)
		storageClient, err := storage.NewClient(ctx, option.WithCredentialsFile(viper.Get("service-account-path").(string)))
		if err != nil {
			panic(fmt.Errorf("error creating cloud storage client: %s", err))
		}

		bktName := viper.Get("cloud-storage-bucket-for-thumbnails").(string)
		bkt := storageClient.Bucket(bktName)
		attrs := storage.BucketAttrs{}
		aclrule := storage.ACLRule{Entity: storage.AllUsers, Role: storage.RoleReader}
		attrs.ACL = append(attrs.ACL, aclrule)

		objName := fmt.Sprintf("%s.png", result.Podcast.ID)
		obj := bkt.Object(objName)
		w := obj.NewWriter(ctx)

		fmt.Println("Writing thumbnail to Cloud Storage.")
		if err := png.Encode(w, *img); err != nil {
			panic(fmt.Errorf("error writing png: %s", err))
		}

		//Also write the data to the Podcast object.
		buf := bytes.NewBuffer([]byte{})
		if err := png.Encode(buf, *img); err != nil {
			panic(fmt.Errorf("error writing thumb png to buffer: %s", err))
		}
		result.Podcast.ImageThumbnail.Data = buf.Bytes()

		if err := w.Close(); err != nil {
			panic(fmt.Errorf("error closing cloud storage object: %s", err))
		}
		if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
			panic(fmt.Errorf("error making image publicly viewable: %s", err))
		}

		thumbURL := fmt.Sprintf("%s%s/%s", "https://storage.googleapis.com/", bktName, objName)
		if _, err := url.Parse(thumbURL); err != nil {
			panic(fmt.Errorf("error building url to thumbnail image"))
		}

		result.Podcast.ImageThumbnail.Title = result.Podcast.ImageOriginal.Title
		result.Podcast.ImageThumbnail.URL = thumbURL

		fmt.Println("Podcast thumbnail available: ", thumbURL)

		//Create the Podcast document in podcasts collection.
		_, err = docRef.Set(ctx, result.Podcast)
		if err != nil {
			fmt.Println(fmt.Errorf("Couldn't create podcast document in db: %s", err))
		}
		fmt.Printf("Created new Podcast: %s by %s\n", result.Podcast.Title, result.Podcast.Author.Name)
	}

	//Batch write episodes to firestore
	batch := client.Batch()
	i := 0
	for _, ep := range result.Episodes {
		ref := client.Collection("podcasts").Doc(result.Podcast.ID).Collection("episodes").Doc(ep.ID)
		batch.Set(ref, ep)
		if i == 499 { //500 episodes is enough, and is the max Batch put.
			break
		}
	}

	fmt.Printf("Batch writing %d episodes to Firestore...\n", len(result.Episodes))
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

	if feed.FeedLink == "" {
		feed.FeedLink = feedURL
	}

	//sometimes the email is blank, but is in the itunes:extensions.
	if feed.Author != nil {
		if feed.Author.Email == "" && feed.ITunesExt.Owner != nil {
			feed.Author.Email = feed.ITunesExt.Owner.Email
		}
	} else {
		feed.Author = &gofeed.Person{Email: feed.ITunesExt.Owner.Email, Name: feed.ITunesExt.Owner.Name}
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

		ep := NewEpisode(item, r.Podcast.ID)
		if ep != nil {
			r.Episodes = append(r.Episodes, ep)
		}
	}

	//debug
	//data, _ := json.MarshalIndent(r.Podcast, "", "	")
	//fmt.Printf("%s\n", data)

	return &r, nil
}

func prompt(ctx context.Context) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter Feed URL: ")
	text, _ := reader.ReadString('\n')
	return strings.TrimSpace(text)
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

	text := prompt(ctx)

	if text == "delete" {
		if err := deleteCollection(ctx, client, client.Collection("podcasts"), 500); err != nil {
			panic(fmt.Errorf("error: couldn't delete podcasts collection, error: %s", err))
		}
		return
	}

	if text == "rebuild" {
		if err := deleteCollection(ctx, client, client.Collection("podcasts"), 500); err != nil {
			panic(fmt.Errorf("error: couldn't delete podcasts collection, error: %s", err))
		}

		for _, feed := range feeds {
			result, err := loadRSSFeed(feed)
			if err != nil {
				panic(fmt.Errorf("error: %s", err))
			}
			insert(ctx, client, result)
		}
	} else if text == "new" {

		for _, feed := range feeds {

			fmt.Println("Checking for ", feed)
			ref, err := client.Collection("podcasts").Doc(md5hash(feed)).Get(ctx)
			if ref != nil {
				// this one exists... keep going.
				fmt.Println("Skipping ", feed)
				continue
			}
			if err != nil && grpc.Code(err) != codes.NotFound {
				panic(fmt.Errorf("error: %s", err))
			}

			result, err := loadRSSFeed(feed)
			if err != nil {
				panic(fmt.Errorf("error: %s", err))
			}
			insert(ctx, client, result)
		}

	} else {
		result, err := loadRSSFeed(text)
		if err != nil {
			panic(fmt.Errorf("error: %s", err))
		}
		insert(ctx, client, result)
	}

}
