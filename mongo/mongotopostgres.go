package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/joho/godotenv"
	"github.com/lib/pq"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var pgPool *pgxpool.Pool
var mongoClient *mongo.Client

func main() {
	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	ctx := context.Background()

	// Get MongoDB URI and PostgreSQL URI from environment variables
	mongoURI := os.Getenv("MONGO_URI")
	pgURI := os.Getenv("PG_URI")

	// Connect to MongoDB
	var err error
	mongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	// Connect to PostgreSQL
	pgPool, err = pgxpool.Connect(ctx, pgURI)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pgPool.Close()

	// Create tables if they do not exist
	if err := createTables(ctx, pgPool); err != nil {
		log.Fatalf("Failed to create tables: %v", err)
	}

	// Transfer collections
	if err := transferData(ctx); err != nil {
		log.Fatalf("Failed to transfer data: %v", err)
	}
}

func createTables(ctx context.Context, pgPool *pgxpool.Pool) error {
	tableCreationQueries := []string{
		`CREATE TABLE IF NOT EXISTS BlogPost (
			id SERIAL PRIMARY KEY,
			slug TEXT UNIQUE,
			title TEXT,
			date TIMESTAMP,
			authorName TEXT,
			overview TEXT,
			authorAvatar TEXT,
			content TEXT[]
		)`,
		`CREATE TABLE IF NOT EXISTS Partner (
			id SERIAL PRIMARY KEY,
			banner TEXT,
			logo TEXT,
			title TEXT UNIQUE,
			text TEXT,
			link TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS "User" (
			id TEXT PRIMARY KEY,
			username TEXT,
			displayName TEXT,
			userId INT,
			email TEXT UNIQUE,
			createdAt TIMESTAMP,
			profilePicture TEXT,
			profileBanner TEXT,
			bio TEXT,
			IsVerified BOOLEAN,
			isOrganisation BOOLEAN,
			isDeveloper BOOLEAN,
			isPartner BOOLEAN,
			isOwner BOOLEAN,
			isBanned BOOLEAN,
			password TEXT,
			links TEXT[],
			followers TEXT[],
			following TEXT[]
		)`,
		`CREATE TABLE IF NOT EXISTS coterie (
			id TEXT PRIMARY KEY,  -- Using TEXT for the ID
			name TEXT NOT NULL,
			description TEXT,
			members TEXT[],  -- Array of TEXT to store member usernames
			owner TEXT NOT NULL,  -- Owner ID as TEXT
			createdAt TIMESTAMP WITH TIME ZONE NOT NULL,
			banner TEXT,
			avatar TEXT,
			roles JSONB,  -- JSONB for roles
			bannedMembers TEXT[],  -- Array of TEXT for banned members
			warningDetails JSONB,  -- JSONB for warning details
			warningLimit INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS Post (
			id TEXT PRIMARY KEY,
			author TEXT,
			title TEXT,
			content TEXT,
			coterie TEXT,
			createdAt TIMESTAMP,
			image TEXT,
			hearts TEXT[],
			comments JSONB
	);`,
	}

	for _, query := range tableCreationQueries {
		if _, err := pgPool.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create table: %v", err)
		}
	}

	return nil
}

func transferData(ctx context.Context) error {
	collections := []struct {
		name         string
		collection   *mongo.Collection
		transferFunc func(ctx context.Context, collection *mongo.Collection, pgPool *pgxpool.Pool) error
	}{
		{"partners", mongoClient.Database("SocialFlux").Collection("partners"), transferPartners},
		{"blogposts", mongoClient.Database("SocialFlux").Collection("blogposts"), transferBlogPosts},
		{"users", mongoClient.Database("SocialFlux").Collection("users"), transferUsers},
		{"coterie", mongoClient.Database("SocialFlux").Collection("coterie"), transferCoteries},
		{"posts", mongoClient.Database("SocialFlux").Collection("posts"), transferPosts},
	}

	for _, col := range collections {
		start := time.Now()
		log.Printf("Starting transfer for collection %s...", col.name)
		if err := col.transferFunc(ctx, col.collection, pgPool); err != nil {
			log.Printf("Failed to transfer %s: %v", col.name, err)
		} else {
			elapsed := time.Since(start)
			log.Printf("Successfully transferred %s in %v", col.name, elapsed)
		}
	}

	return nil
}

func transferBlogPosts(ctx context.Context, collection *mongo.Collection, pgPool *pgxpool.Pool) error {
	cursor, err := collection.Find(ctx, bson.D{})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var blogPost struct {
			Slug         string `bson:"slug"`
			Title        string `bson:"title"`
			Date         string `bson:"date"`
			AuthorName   string `bson:"authorName"`
			Overview     string `bson:"overview"`
			AuthorAvatar string `bson:"authorAvatar"`
			Content      []struct {
				Body string `bson:"body"`
			} `bson:"content"`
		}
		if err := cursor.Decode(&blogPost); err != nil {
			return err
		}

		// Parse the date
		date, err := time.Parse("January 02, 2006", blogPost.Date)
		if err != nil {
			return fmt.Errorf("error parsing date %s: %v", blogPost.Date, err)
		}

		// Check if the blog post already exists in PostgreSQL
		var existingID int
		err = pgPool.QueryRow(ctx, "SELECT id FROM BlogPost WHERE slug = $1", blogPost.Slug).Scan(&existingID)
		if err == nil {
			// Data already exists, skip insertion
			continue
		} else if err != pgx.ErrNoRows {
			return err
		}

		// Flatten the content array into a single string or handle as needed
		var contentStrings []string
		for _, item := range blogPost.Content {
			contentStrings = append(contentStrings, item.Body)
		}

		_, err = pgPool.Exec(ctx, `INSERT INTO BlogPost (slug, title, date, authorName, overview, authorAvatar, content) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			blogPost.Slug, blogPost.Title, date, blogPost.AuthorName, blogPost.Overview, blogPost.AuthorAvatar, contentStrings)
		if err != nil {
			return err
		}
	}

	return nil
}

func transferPartners(ctx context.Context, collection *mongo.Collection, pgPool *pgxpool.Pool) error {
	cursor, err := collection.Find(ctx, bson.D{})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var partner struct {
			Banner string `bson:"banner,omitempty"`
			Logo   string `bson:"logo,omitempty"`
			Title  string `bson:"title,omitempty"`
			Text   string `bson:"text,omitempty"`
			Link   string `bson:"link,omitempty"`
		}
		if err := cursor.Decode(&partner); err != nil {
			return err
		}

		// Check if the partner already exists in PostgreSQL
		var existingID int
		err = pgPool.QueryRow(ctx, `SELECT id FROM Partner WHERE title = $1`, partner.Title).Scan(&existingID)
		if err == nil {
			// Data already exists, skip insertion
			continue
		} else if err != pgx.ErrNoRows {
			return err
		}

		_, err = pgPool.Exec(ctx, `INSERT INTO Partner (banner, logo, title, text, link) VALUES ($1, $2, $3, $4, $5)`,
			partner.Banner, partner.Logo, partner.Title, partner.Text, partner.Link)
		if err != nil {
			return err
		}
	}

	return nil
}

func transferUsers(ctx context.Context, collection *mongo.Collection, pgPool *pgxpool.Pool) error {
	cursor, err := collection.Find(ctx, bson.D{})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var user struct {
			ID             string    `bson:"_id"`
			Username       string    `bson:"username"`
			DisplayName    string    `bson:"displayName"`
			UserID         int       `bson:"userid"`
			Email          string    `bson:"email"`
			CreatedAt      time.Time `bson:"createdAt"`
			ProfilePicture string    `bson:"profilePicture"`
			ProfileBanner  string    `bson:"profileBanner"`
			Bio            string    `bson:"bio"`
			IsVerified     bool      `bson:"IsVerified"`
			IsOrganisation bool      `bson:"isOrganisation"`
			IsDeveloper    bool      `bson:"isDeveloper"`
			IsPartner      bool      `bson:"isPartner"`
			IsOwner        bool      `bson:"isOwner"`
			IsBanned       bool      `bson:"isBanned"`
			Password       string    `bson:"password"`
			Links          []string  `bson:"links"`
			Followers      []string  `bson:"followers"`
			Following      []string  `bson:"following"`
		}
		if err := cursor.Decode(&user); err != nil {
			return err
		}

		// Check if the user already exists in PostgreSQL
		var existingID string
		err = pgPool.QueryRow(ctx, "SELECT id FROM \"User\" WHERE email = $1", user.Email).Scan(&existingID)
		if err == nil {
			// Data already exists, skip insertion
			continue
		} else if err != pgx.ErrNoRows {
			return err
		}

		_, err = pgPool.Exec(ctx, `INSERT INTO "User" (id, username, displayName, userid, email, createdAt, profilePicture, profileBanner, bio, IsVerified, isOrganisation, isDeveloper, isPartner, isOwner, isBanned, password, links, followers, following) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)`,
			user.ID, user.Username, user.DisplayName, user.UserID, user.Email, user.CreatedAt, user.ProfilePicture, user.ProfileBanner, user.Bio, user.IsVerified, user.IsOrganisation, user.IsDeveloper, user.IsPartner, user.IsOwner, user.IsBanned, user.Password, pq.Array(user.Links), pq.Array(user.Followers), pq.Array(user.Following))
		if err != nil {
			return err
		}
	}

	return nil
}

type Roles struct {
	Owner     []string `json:"owners"`
	Moderator []string `json:"moderators"`
	Admin     []string `json:"admins"`
}

type WarningDetail struct {
	Reason string    `bson:"reason" json:"reason"`
	Time   time.Time `bson:"time" json:"time"`
}

func transferCoteries(ctx context.Context, collection *mongo.Collection, pgPool *pgxpool.Pool) error {
	cursor, err := collection.Find(ctx, bson.D{})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var coterie struct {
			ID              primitive.ObjectID         `bson:"_id" json:"_id"`
			Name            string                     `bson:"name" json:"name"`
			Description     string                     `bson:"description" json:"description"`
			Members         []string                   `bson:"members" json:"members"`
			Owner           primitive.ObjectID         `bson:"owner" json:"owner"`
			CreatedAt       time.Time                  `bson:"createdAt" json:"createdAt"`
			Banner          string                     `bson:"banner" json:"banner,omitempty"`
			Avatar          string                     `bson:"avatar" json:"avatar,omitempty"`
			Roles           map[string][]string        `bson:"roles,omitempty" json:"roles,omitempty"`
			BannedMembers   []string                   `bson:"bannedMembers,omitempty" json:"bannedMembers,omitempty"`
			MemberUsernames []string                   `json:"memberUsernames,omitempty"`
			WarningDetails  map[string][]WarningDetail `bson:"warningDetails,omitempty" json:"warningDetails,omitempty"`
			WarningLimit    int                        `bson:"warningLimit" json:"warningLimit"`
		}
		if err := cursor.Decode(&coterie); err != nil {
			return err
		}

		// Check if the coterie already exists in PostgreSQL
		var existingID string
		err = pgPool.QueryRow(ctx, `SELECT id FROM coterie WHERE id = $1`, coterie.ID).Scan(&existingID)
		if err == nil {
			// Data already exists, skip insertion
			continue
		} else if err != pgx.ErrNoRows {
			return err
		}

		_, err = pgPool.Exec(ctx, `INSERT INTO coterie (id, name, description, members, owner, createdAt, banner, avatar, roles, bannedMembers, warningDetails, warningLimit) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			coterie.ID.Hex(), coterie.Name, coterie.Description, pq.Array(coterie.Members), coterie.Owner.Hex(), coterie.CreatedAt, coterie.Banner, coterie.Avatar, coterie.Roles, pq.Array(coterie.BannedMembers), coterie.WarningDetails, coterie.WarningLimit)
		if err != nil {
			return err
		}
	}

	return nil
}

type Comment struct {
	ID      string `bson:"_id,omitempty" json:"_id,omitempty"`
	Content string `bson:"content" json:"content"`
	Author  string `bson:"author" json:"author"`
}

func transferPosts(ctx context.Context, collection *mongo.Collection, pgPool *pgxpool.Pool) error {
	cursor, err := collection.Find(ctx, bson.D{})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var post struct {
			ID        string    `bson:"_id"`
			Author    string    `bson:"author"`
			Title     string    `bson:"title"`
			Image     string    `bson:"image,omitempty"`
			Content   string    `bson:"content"`
			Hearts    []string  `bson:"hearts"`
			Comments  []Comment `bson:"comments,omitempty"`
			Coterie   string    `bson:"coterie"`
			CreatedAt time.Time `bson:"createdAt"`
		}

		if err := cursor.Decode(&post); err != nil {
			return err
		}

		// Check if the post already exists in PostgreSQL
		var existingID string
		err = pgPool.QueryRow(ctx, "SELECT id FROM Post WHERE id = $1", post.ID).Scan(&existingID)
		if err == nil {
			// Data already exists, skip insertion
			continue
		} else if err != pgx.ErrNoRows {
			return err
		}

		// Insert the post into PostgreSQL
		_, err = pgPool.Exec(ctx, "INSERT INTO Post (id, author, title, content, coterie, createdAt, image, hearts, comments) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
			post.ID, post.Author, post.Title, post.Content, post.Coterie, post.CreatedAt, post.Image, pq.Array(post.Hearts), post.Comments)
		if err != nil {
			return err
		}
	}

	return nil
}
