require 'sqlite3'
require 'pathname'

DB_NAME = "contacts.db"

DATA_DIR = Pathname.new(__FILE__).dirname.join('db')

Dir.mkdir DATA_DIR unless DATA_DIR.exist?

DB_FILENAME = DATA_DIR.join(DB_NAME)

puts "Saving DB in #{DB_FILENAME}"
$db = SQLite3::Database.new DB_FILENAME.to_s

module ContactsDB

  def self.drop
    puts "DROPPING THE TABLES"
    $db.execute("DROP TABLE IF EXISTS contacts");
    $db.execute("DROP TABLE IF EXISTS addresses");
  end

  def self.setup
    puts "CREATING THE TABLES"
    # Plain SQL Execution
    $db.execute(
      <<-SQL
      CREATE TABLE contacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name VARCHAR(64) NOT NULL,
        last_name VARCHAR(64) NOT NULL,
        phone VARCHAR(128),
        email VARCHAR(128),
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME
      );
      SQL
    )

    $db.execute(
      <<-SQL
      CREATE TABLE addresses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        contact_id INTEGER NOT NULL,
        street VARCHAR(64),
        street_2 VARCHAR(64),
        city VARCHAR(64),
        state VARCHAR(2),
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME
      );
      SQL
    )
  end

  def self.seed_contacts

    puts "SEEDING THE TABLES"

    contact_ids = []
    # SQL execution with placeholders

    $db.execute("INSERT INTO contacts(first_name, last_name) VALUES (?,?)", ['Marie', 'Curie'])
    contact_ids << $db.execute("SELECT  last_insert_rowid();").first #REMEMBER .FIRST!

    #This is repetitive and inefficient
    $db.execute("INSERT INTO contacts(first_name, last_name) VALUES (?,?)", ['Albert', 'Einstein'])
    contact_ids << $db.execute("SELECT  last_insert_rowid();").first

    #This is repetitive and inefficient
    $db.execute("INSERT INTO contacts(first_name, last_name) VALUES (?,?)", ['Nicola', 'Tesla'])
    contact_ids << $db.execute("SELECT  last_insert_rowid();").first

    #Return all the contact_ids
    contact_ids
  end


  def self.seed_addresses ids, addresses
    begin
      inserter = $db.prepare(
        "INSERT INTO addresses (contact_id, street, city, state) VALUES (?,?,?,?)"
      )
      ids.each_with_index do |id, index|
        address_array = addresses[index]
        address_array.each do |address|
          inserter.execute [id, address[:street], address[:city], address[:state]]
        end
      end
    rescue SQLite3::Exception => e
      puts "Exception occurred"
      puts e
    ensure
      inserter.close if inserter
    end
  end

  def self.update ids, emails
    begin
      updater = $db.prepare(
        "UPDATE contacts set email = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
      )
      ids.each_with_index do |id, index|
        updater.execute [emails[index], id]
      end
    rescue SQLite3::Exception => e
      puts "Exception occurred"
      puts e
    ensure
      updater.close if updater
    end
  end

  def self.dump
    puts "DUMPING CONTACTS"
    contacts = $db.execute("SELECT * from contacts")
    contacts.each do |contact|
      p contact
    end
      puts "DUMPING ADDRESSES"
    addresses = $db.execute("SELECT * from addresses")
    addresses.each do |address|
      p address
    end  
  end
end

work_emails = ['marie@curie.net','al@thestein.com','nicky@tesla.edu']
work_addresses = [
  [
    {street: '1 Main St', city: 'Fresno', state: 'CA'},
    {street: '14 Mellow Way', city: 'Sacramento', state: 'CA'}
  ],
  [
    {street: '14 West Drive', city: 'Madison',state: 'WI'}
  ],
  []
]


ContactsDB.drop
ContactsDB.setup
contact_ids = ContactsDB.seed_contacts
ContactsDB.dump
ContactsDB.seed_addresses contact_ids, work_addresses
ContactsDB.dump
ContactsDB.update contact_ids, work_emails
ContactsDB.dump

#Notes to Stu:
# Talk about separate terminal shell for sqlite
# Talk about joins (http://blog.codinghorror.com/a-visual-explanation-of-sql-joins/)
  # INNER, LEFT, RIGHT, FULL OUTER
  # - Cartesian - select * from contacts, addresses 
# http://www.sqlite.org/omitted.html

