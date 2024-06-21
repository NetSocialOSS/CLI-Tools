package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Post struct {
	ID        string    `bson:"_id" json:"_id"`
	Title     string    `bson:"title" json:"title"`
	Content   string    `bson:"content" json:"content"`
	Author    string    `bson:"author" json:"author"`
	ImageURL  string    `bson:"imageUrl,omitempty" json:"imageUrl,omitempty"`
	Image     string    `bson:"image,omitempty" json:"image,omitempty"`
	Hearts    []string  `bson:"hearts" json:"hearts"`
	CreatedAt time.Time `bson:"createdAt" json:"createdAt"`
	Comments  []Comment `bson:"comments,omitempty" json:"comments,omitempty"`
}

type Comment struct {
	ID             string    `bson:"_id,omitempty" json:"_id,omitempty"`
	Content        string    `bson:"content" json:"content"`
	Author         string    `bson:"author" json:"author"`
	IsVerified     bool      `json:"isVerified"`
	IsOrganisation bool      `json:"isOrganisation"`
	IsPartner      bool      `json:"isPartner"`
	AuthorName     string    `json:"authorName"`
	IsOwner        bool      `json:"isOwner"`
	IsDeveloper    bool      `json:"isDeveloper"`
	Replies        []Comment `bson:"replies" json:"replies"`
}

type User struct {
	ID             string    `bson:"_id" json:"_id"`
	Username       string    `bson:"username" json:"username"`
	DisplayName    string    `bson:"displayname" json:"displayname"`
	UserID         int       `bson:"userid" json:"userid"`
	Email          string    `bson:"email" json:"email"`
	CreatedAt      time.Time `bson:"createdAt" json:"createdAt"`
	ProfilePicture string    `bson:"profilePicture" json:"profilePicture"`
	ProfileBanner  string    `bson:"profileBanner" json:"profileBanner"`
	Bio            string    `bson:"bio" json:"bio"`
	IsVerified     bool      `json:"isVerified"`
	IsOrganisation bool      `json:"isOrganisation"`
	IsDeveloper    bool      `json:"isDeveloper"`
	IsPartner      bool      `json:"isPartner"`
	IsOwner        bool      `json:"isOwner"`
	Password       string    `bson:"password,omitempty" json:"-"`
	Links          []string  `bson:"links,omitempty" json:"links,omitempty"`
}

type Partner struct {
	Banner string `json:"banner,omitempty" bson:"banner,omitempty"`
	Logo   string `json:"logo,omitempty" bson:"logo,omitempty"`
	Title  string `json:"title,omitempty" bson:"title,omitempty"`
	Text   string `json:"text,omitempty" bson:"text,omitempty"`
	Link   string `json:"link,omitempty" bson:"link,omitempty"`
}

type BlogPost struct {
	Slug         string      `json:"slug"`
	Title        string      `json:"title"`
	Date         string      `json:"date"`
	AuthorName   string      `json:"authorname"`
	Overview     string      `json:"overview"`
	Authoravatar string      `json:"authoravatar"`
	Content      []PostEntry `json:"content"`
}

type PostEntry struct {
	Body string `json:"body"`
}

func main() {
	// Load environment variables
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	mongodbURI := os.Getenv("MONGODB_URI")
	mysqlURI := os.Getenv("MYSQL_URI")

	// Connect to MongoDB
	mongoClient, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongodbURI))
	if err != nil {
		log.Fatalf("Error connecting to MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(context.TODO())

	// Connect to MySQL
	mysqlDB, err := sql.Open("mysql", mysqlURI)
	if err != nil {
		log.Fatalf("Error connecting to MySQL: %v", err)
	}
	defer mysqlDB.Close()

	if err = mysqlDB.Ping(); err != nil {
		log.Fatalf("MySQL ping failed: %v", err)
	}

	// Collections in MongoDB
	postsCollection := mongoClient.Database("SocialFlux").Collection("posts")
	usersCollection := mongoClient.Database("SocialFlux").Collection("users")
	partnersCollection := mongoClient.Database("SocialFlux").Collection("partners")
	blogsCollection := mongoClient.Database("SocialFlux").Collection("blogs")

	// Fetch and migrate posts
	migratePosts(postsCollection, mysqlDB)
	// Fetch and migrate users
	migrateUsers(usersCollection, mysqlDB)
	// Fetch and migrate partners
	migratePartners(partnersCollection, mysqlDB)
	// Fetch and migrate blogs
	migrateBlogs(blogsCollection, mysqlDB)
}

func migratePosts(postsCollection *mongo.Collection, mysqlDB *sql.DB) {
	cursor, err := postsCollection.Find(context.TODO(), bson.M{})
	if err != nil {
		log.Fatalf("Error finding posts: %v", err)
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		var post Post
		if err := cursor.Decode(&post); err != nil {
			log.Fatalf("Error decoding post: %v", err)
		}
		// Insert into MySQL
		query := "INSERT INTO posts (id, title, content, author, image_url, image, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
		_, err := mysqlDB.Exec(query, post.ID, post.Title, post.Content, post.Author, post.ImageURL, post.Image, post.CreatedAt)
		if err != nil {
			log.Fatalf("Error inserting post into MySQL: %v", err)
		}
	}
}

func migrateUsers(usersCollection *mongo.Collection, mysqlDB *sql.DB) {
	cursor, err := usersCollection.Find(context.TODO(), bson.M{})
	if err != nil {
		log.Fatalf("Error finding users: %v", err)
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		var user User
		if err := cursor.Decode(&user); err != nil {
			log.Fatalf("Error decoding user: %v", err)
		}
		// Insert into MySQL
		query := "INSERT INTO users (id, username, display_name, user_id, email, created_at, profile_picture, profile_banner, bio, is_verified, is_organisation, is_developer, is_partner, is_owner, password) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		_, err := mysqlDB.Exec(query, user.ID, user.Username, user.DisplayName, user.UserID, user.Email, user.CreatedAt, user.ProfilePicture, user.ProfileBanner, user.Bio, user.IsVerified, user.IsOrganisation, user.IsDeveloper, user.IsPartner, user.IsOwner, user.Password)
		if err != nil {
			log.Fatalf("Error inserting user into MySQL: %v", err)
		}
	}
}

func migratePartners(partnersCollection *mongo.Collection, mysqlDB *sql.DB) {
	cursor, err := partnersCollection.Find(context.TODO(), bson.M{})
	if err != nil {
		log.Fatalf("Error finding partners: %v", err)
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		var partner Partner
		if err := cursor.Decode(&partner); err != nil {
			log.Fatalf("Error decoding partner: %v", err)
		}
		// Insert into MySQL
		query := "INSERT INTO partners (banner, logo, title, text, link) VALUES (?, ?, ?, ?, ?)"
		_, err := mysqlDB.Exec(query, partner.Banner, partner.Logo, partner.Title, partner.Text, partner.Link)
		if err != nil {
			log.Fatalf("Error inserting partner into MySQL: %v", err)
		}
	}
}

func migrateBlogs(blogsCollection *mongo.Collection, mysqlDB *sql.DB) {
	cursor, err := blogsCollection.Find(context.TODO(), bson.M{})
	if err != nil {
		log.Fatalf("Error finding blogs: %v", err)
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		var blog BlogPost
		if err := cursor.Decode(&blog); err != nil {
			log.Fatalf("Error decoding blog: %v", err)
		}
		// Insert into MySQL
		query := "INSERT INTO blogs (slug, title, date, author_name, overview, author_avatar) VALUES (?, ?, ?, ?, ?, ?)"
		_, err := mysqlDB.Exec(query, blog.Slug, blog.Title, blog.Date, blog.AuthorName, blog.Overview, blog.Authoravatar)
		if err != nil {
			log.Fatalf("Error inserting blog into MySQL: %v", err)
		}

		for _, entry := range blog.Content {
			entryQuery := "INSERT INTO blog_entries (blog_slug, body) VALUES (?, ?)"
			_, err := mysqlDB.Exec(entryQuery, blog.Slug, entry.Body)
			if err != nil {
				log.Fatalf("Error inserting blog entry into MySQL: %v", err)
			}
		}
	}
}
