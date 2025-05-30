const { MongoClient } = require('mongodb');

const uri = 'mongodb+srv://botsaibot:%40Botsaibot7@cluster0.tmzy54p.mongodb.net/plp_bookstore?retryWrites=true&w=majority';
const client = new MongoClient(uri);

async function run() {
  try {
    await client.connect();
    const db = client.db('plp_bookstore');
    const books = db.collection('books');

    // ------------------------------
    // Task 2: Basic CRUD Operations
    // ------------------------------

    // Find all books in a specific genre
    const genreBooks = await books.find({ genre: "Fantasy" }).toArray();
    console.log("Books in Fantasy genre:", genreBooks);

    // Find books published after a certain year
    const recentBooks = await books.find({ published_year: { $gt: 2010 } }).toArray();
    console.log("Books published after 2010:", recentBooks);

    // Find books by a specific author
    const authorBooks = await books.find({ author: "George Orwell" }).toArray();
    console.log("Books by George Orwell:", authorBooks);

    // Update the price of a specific book
    const updatePrice = await books.updateOne(
      { title: "1984" },
      { $set: { price: 25.99 } }
    );
    console.log("Price update result:", updatePrice);

    // Delete a book by its title
    const deleteBook = await books.deleteOne({ title: "Moby Dick" });
    console.log("Book deletion result:", deleteBook);

    // ------------------------------
    // Task 3: Advanced Queries
    // ------------------------------

    // Find books that are both in stock and published after 2010
    const inStockRecent = await books.find({
      in_stock: true,
      published_year: { $gt: 2010 }
    }).toArray();
    console.log("In stock & published after 2010:", inStockRecent);

    // Projection: return only title, author, and price fields
    const projectedBooks = await books.find({}, {
      projection: { title: 1, author: 1, price: 1, _id: 0 }
    }).toArray();
    console.log("Projected fields:", projectedBooks);

    // Sort by price ascending
    const priceAsc = await books.find().sort({ price: 1 }).toArray();
    console.log("Books sorted by price (ascending):", priceAsc);

    // Sort by price descending
    const priceDesc = await books.find().sort({ price: -1 }).toArray();
    console.log("Books sorted by price (descending):", priceDesc);

    // Pagination (5 books per page)
    const page = 1; // Change page number here for different pages
    const perPage = 5;
    const paginatedBooks = await books.find()
      .skip((page - 1) * perPage)
      .limit(perPage)
      .toArray();
    console.log(`Books on page ${page}:`, paginatedBooks);

    // ------------------------------
    // Task 4: Aggregation Pipeline
    // ------------------------------

    // Average price by genre
    const avgPriceByGenre = await books.aggregate([
      { $group: { _id: "$genre", averagePrice: { $avg: "$price" } } }
    ]).toArray();
    console.log("Average price by genre:", avgPriceByGenre);

    // Author with the most books
    const topAuthor = await books.aggregate([
      { $group: { _id: "$author", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log("Author with most books:", topAuthor);

    // Group books by publication decade
    const booksByDecade = await books.aggregate([
      {
        $group: {
          _id: {
            $concat: [
              { $substr: ["$published_year", 0, 3] }, // first 3 digits (e.g., "199")
              "0s"
            ]
          },
          count: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log("Books grouped by decade:", booksByDecade);

    // ------------------------------
    // Task 5: Indexing
    // ------------------------------

    // Create index on title for faster searches
    await books.createIndex({ title: 1 });
    console.log("Index on title created.");

    // Create compound index on author and published_year
    await books.createIndex({ author: 1, published_year: -1 });
    console.log("Compound index on author and published_year created.");

    // Use explain to show performance improvement with index
    const explained = await books.find({ title: "1984" }).explain("executionStats");
    console.log("Explain output:", explained.executionStats);

  } finally {
    await client.close();
  }
}

run().catch(console.dir);
