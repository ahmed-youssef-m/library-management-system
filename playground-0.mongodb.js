// ------------------ 1. Create Collections ------------------
use("library_db");
db.createCollection("members");
db.createCollection("books");
db.createCollection("borrowings");

// ------------------ 2. Insert Sample Data ------------------
use("library_db");
db.members.insertMany([
  { name: "Alice", age: 25, membership_type: "Gold", join_year: 2020 },
  { name: "Bob", age: 30, membership_type: "Silver", join_year: 2019 },
  { name: "Charlie", age: 28, membership_type: "Gold", join_year: 2021 },
  { name: "Diana", age: 22, membership_type: "Bronze", join_year: 2022 },
  { name: "Evan", age: 35, membership_type: "Silver", join_year: 2018 },
]);

use("library_db");
db.books.insertMany([
  {
    title: "Modern Egypt",
    author: "John Doe",
    genre: "History",
    year_published: 2005,
  },
  {
    title: "Python Basics",
    author: "Jane Smith",
    genre: "Programming",
    year_published: 2015,
  },
  {
    title: "Deep Space",
    author: "Arthur King",
    genre: "Science Fiction",
    year_published: 2010,
  },
]);

use("library_db");
db.borrowings.insertMany([
  {
    member_id: db.members.findOne({ name: "Alice" })._id,
    book_id: db.books.findOne({ title: "Modern Egypt" })._id,
    borrow_date: ISODate("2024-05-01T00:00:00Z"),
    return_date: null,
  },
  {
    member_id: db.members.findOne({ name: "Bob" })._id,
    book_id: db.books.findOne({ title: "Python Basics" })._id,
    borrow_date: ISODate("2024-05-05T00:00:00Z"),
    return_date: null,
  },
  {
    member_id: db.members.findOne({ name: "Charlie" })._id,
    book_id: db.books.findOne({ title: "Modern Egypt" })._id,
    borrow_date: ISODate("2024-05-07T00:00:00Z"),
    return_date: null,
  },
]);

// ------------------ 3. CRUD Operations ------------------

// Insert a new member
use("library_db");
db.members.insertOne({
  name: "Frank",
  age: 27,
  membership_type: "Gold",
  join_year: 2023,
});

// Update a member
use("library_db");
db.members.updateOne({ name: "Alice" }, { $set: { age: 26 } });

// Delete a member (and manually cascade delete borrowings)
use("library_db");
var memberToDelete = db.members.findOne({ name: "Evan" });
db.borrowings.deleteMany({ member_id: memberToDelete._id });
db.members.deleteOne({ _id: memberToDelete._id });

// Update book genre
use("library_db");
db.books.updateOne(
  { title: "Python Basics" },
  { $set: { genre: "Computer Science" } }
);

// Delete book and its borrowings
use("library_db");
var bookToDelete = db.books.findOne({ title: "Deep Space" });
db.borrowings.deleteMany({ book_id: bookToDelete._id });
db.books.deleteOne({ _id: bookToDelete._id });

// Insert a new borrowing
use("library_db");
db.borrowings.insertOne({
  member_id: db.members.findOne({ name: "Diana" })._id,
  book_id: db.books.findOne({ title: "Modern Egypt" })._id,
  borrow_date: new Date(),
  return_date: null,
});

// Update return date of a borrowing
use("library_db");
db.borrowings.updateOne(
  {
    member_id: db.members.findOne({ name: "Alice" })._id,
    book_id: db.books.findOne({ title: "Modern Egypt" })._id,
  },
  { $set: { return_date: new Date() } }
);

// ------------------ 4. Queries ------------------

// Members who borrowed "Modern Egypt"
use("library_db");
var book = db.books.findOne({ title: "Modern Egypt" });
var borrowingRecords = db.borrowings.find({ book_id: book._id }).toArray();
var memberIds = borrowingRecords.map((b) => b.member_id);
db.members.find({ _id: { $in: memberIds } });

// Members who joined before 2020
use("library_db");
db.members.find({ join_year: { $lt: 2020 } });

// Books borrowed by more than 2 different members
use("library_db");
db.borrowings.aggregate([
  {
    $group: {
      _id: "$book_id",
      uniqueMembers: { $addToSet: "$member_id" },
      count: { $sum: 1 },
    },
  },
  { $match: { count: { $gt: 2 } } },
]);

// Books borrowed by Alice
use("library_db");
var alice = db.members.findOne({ name: "Alice" });
var aliceBorrowings = db.borrowings.find({ member_id: alice._id }).toArray();
var bookIds = aliceBorrowings.map((b) => b.book_id);
db.books.find({ _id: { $in: bookIds } });

// ------------------ 5. Aggregations ------------------

// Total books borrowed per member
use("library_db");
db.borrowings.aggregate([
  { $group: { _id: "$member_id", totalBorrowed: { $sum: 1 } } },
]);

// Average number of books borrowed per membership type
use("library_db");
db.borrowings.aggregate([
  {
    $lookup: {
      from: "members",
      localField: "member_id",
      foreignField: "_id",
      as: "member_info",
    },
  },
  { $unwind: "$member_info" },
  { $group: { _id: "$member_info.membership_type", avgBorrowed: { $avg: 1 } } },
]);

// Members who borrowed more than 1 book
use("library_db");
db.borrowings.aggregate([
  { $group: { _id: "$member_id", total: { $sum: 1 } } },
  { $match: { total: { $gt: 1 } } },
]);

// Group members by membership type and count
use("library_db");
db.members.aggregate([
  { $group: { _id: "$membership_type", totalMembers: { $sum: 1 } } },
]);
