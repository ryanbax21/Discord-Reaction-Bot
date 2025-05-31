import discord
import sqlite3
import nest_asyncio
import os
from collections import Counter
from dotenv import load_dotenv
load_dotenv()
# --- Configuration ---
# IMPORTANT: DO NOT hardcode your bot token here in production.
# Use environment variables for security.
# Set an environment variable named 'DISCORD_BOT_TOKEN' with your actual bot token.
DISCORD_BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
DATABASE_NAME = 'reactions.db'

# Define Discord Intents required for the bot to function.
intents = discord.Intents.default()
intents.members = True # Required to get member information for leaderboards
intents.message_content = True # Required to read message content for commands
intents.reactions = True # Required to receive reaction add/remove events

# Initialize the Discord client with the specified intents.
bot = discord.Bot(intents=intents)

# --- Database Setup ---

def setup_database():
    """
    Connects to the SQLite database and creates necessary tables if they don't exist.
    """
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Create 'users' table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            discriminator TEXT,
            UNIQUE(user_id)
        )
    ''')

    # Create 'messages' table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            message_id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            guild_id INTEGER NOT NULL,
            author_id INTEGER NOT NULL,
            UNIQUE(message_id)
        )
    ''')

    # Create 'reaction_events' table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reaction_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reactor_user_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            message_author_id INTEGER NOT NULL,
            emoji_name TEXT NOT NULL,
            emoji_id INTEGER, -- NULL for unicode emojis
            event_type TEXT NOT NULL, -- 'add' or 'remove'
            guild_id INTEGER, -- <--- Make sure this is present!
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (reactor_user_id) REFERENCES users(user_id),
            FOREIGN KEY (message_id) REFERENCES messages(message_id),
            FOREIGN KEY (message_author_id) REFERENCES users(user_id)
        )
    ''')

    conn.commit()
    conn.close()

# --- Helper Functions for Database Interaction ---

def get_db_connection():
    """Establishes and returns a database connection."""
    return sqlite3.connect(DATABASE_NAME)

async def update_user_in_db(user: discord.User):
    """Inserts or updates user information in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO users (user_id, username, discriminator) VALUES (?, ?, ?)",
        (user.id, user.name, user.discriminator)
    )
    cursor.execute(
        "UPDATE users SET username = ?, discriminator = ? WHERE user_id = ?",
        (user.name, user.discriminator, user.id)
    )
    conn.commit()
    conn.close()

async def update_message_in_db(message: discord.Message):
    """Inserts or updates message information in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO messages (message_id, channel_id, guild_id, author_id) VALUES (?, ?, ?, ?)",
        (message.id, message.channel.id, message.guild.id, message.author.id)
    )
    conn.commit()
    conn.close()

async def record_reaction_event(
    reactor: discord.User,
    message: discord.Message,
    emoji: discord.Emoji | str,
    event_type: str
):
    """Records a reaction add/remove event in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Ensure the reactor and message author are in the users table
    await update_user_in_db(reactor)
    await update_user_in_db(message.author)
    await update_message_in_db(message)

    emoji_name = str(emoji) if isinstance(emoji, str) else emoji.name
    emoji_id = emoji.id if isinstance(emoji, discord.Emoji) else None

    # Get guild_id from message context
    guild_id = message.guild.id if message.guild else None

    cursor.execute(
        "INSERT INTO reaction_events (reactor_user_id, message_id, message_author_id, emoji_name, emoji_id, event_type, guild_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (reactor.id, message.id, message.author.id, emoji_name, emoji_id, event_type, guild_id)
    )
    conn.commit()
    conn.close()

async def backfill_reactions(guild):
    print(f"Backfilling reactions for guild: {guild.name}")
    for channel in guild.text_channels:
        try:
            async for message in channel.history(limit=None, oldest_first=True):
                for reaction in message.reactions:
                    async for user in reaction.users():
                        if user.bot:
                            continue  # Skip bot users
                        # Check if this reaction is already in the DB (optional, for idempotency)
                        await record_reaction_event(user, message, reaction.emoji, 'add')
            print(f"Finished backfilling channel: {channel.name}")
        except Exception as e:
            print(f"Error in channel {channel.name}: {e}")

# --- Bot Events ---

@bot.event
async def on_ready():
    """
    Event that fires when the bot successfully connects to Discord.
    Sets up the database and prints a confirmation message.
    """
    print(f'Logged in as {bot.user} (ID: {bot.user.id})')
    print('------')
    setup_database() # Ensure database is set up on bot start
    print('Database setup complete.')
    for guild in bot.guilds:
        await backfill_reactions(guild)
    print('Backfill complete.')

@bot.event
async def on_reaction_add(reaction: discord.Reaction, user: discord.User):
    """
    Event that fires when a reaction is added to a message.
    Records the reaction event in the database.
    """
    if user == bot.user: # Ignore reactions from the bot itself
        return

    # No partial message check or fetch

    # Ensure message author is available
    if reaction.message.author is None:
        print(f"Could not determine author for message {reaction.message.id}. Skipping reaction event.")
        return

    print(f'Reaction added: {reaction.emoji} by {user} on message by {reaction.message.author}')
    await record_reaction_event(user, reaction.message, reaction.emoji, 'add')

@bot.event
async def on_reaction_remove(reaction: discord.Reaction, user: discord.User):
    print("on_reaction_add event fired")
    """
    Event that fires when a reaction is removed from a message.
    Records the removal event in the database.
    """
    if user == bot.user: # Ignore reactions from the bot itself
        return

    # Ensure message author is available after fetching
    if reaction.message.author is None:
        print(f"Could not determine author for message {reaction.message.id}. Skipping reaction event.")
        return

    print(f'Reaction removed: {reaction.emoji} by {user} from message by {reaction.message.author}')
    await record_reaction_event(user, reaction.message, reaction.emoji, 'remove')

# --- Slash Commands ---

@bot.slash_command(name="topusers", description="Shows the top 10 users by total reactions received.")
async def topreactionsreceived(ctx: discord.ApplicationContext):
    """
    Implements the command to show the top 10 user leaderboard of total reactions received.
    """
    await ctx.defer() # Acknowledge the command immediately

    if not ctx.guild:
        await ctx.followup.send("This command can only be used in a server.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT
            u.username,
            u.discriminator,
            SUM(CASE WHEN re.event_type = 'add' THEN 1 ELSE -1 END) AS total_reactions
        FROM
            reaction_events re
        JOIN
            users u ON re.message_author_id = u.user_id
        WHERE
            re.guild_id = ?
        GROUP BY
            u.user_id
        ORDER BY
            total_reactions DESC
        LIMIT 10
    ''', (ctx.guild.id,))

    results = cursor.fetchall()
    conn.close()

    if not results:
        await ctx.followup.send("No reaction data available yet for this server.")
        return

    embed = discord.Embed(
        title="🏆 Top 10 Users by Reactions Received 🏆",
        description="Here are the users with the most reactions received:",
        color=discord.Color.gold()
    )

    for i, (username, discriminator, total_reactions) in enumerate(results):
        display_name = f"{username}#{discriminator}" if discriminator != "0" else username
        embed.add_field(
            name=f"#{i+1} {display_name}",
            value=f"Total Reactions: {total_reactions}",
            inline=False
        )

    await ctx.followup.send(embed=embed)

@bot.slash_command(name="mytopreactions", description="Shows your top 10 most used reactions (sent).")
async def myusedreactions(ctx: discord.ApplicationContext):
    """
    Implements the command to show the requesting user's most used reactions (sent).
    """
    await ctx.defer() # Acknowledge the command immediately

    if not ctx.guild:
        await ctx.followup.send("This command can only be used in a server.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT
            emoji_name,
            SUM(CASE WHEN event_type = 'add' THEN 1 ELSE -1 END) AS emoji_count
        FROM
            reaction_events
        WHERE
            reactor_user_id = ? AND guild_id = ?
        GROUP BY
            emoji_name
        ORDER BY
            emoji_count DESC
        LIMIT 10
    ''', (ctx.author.id, ctx.guild.id))

    results = cursor.fetchall()
    conn.close()

    if not results:
        await ctx.followup.send("You haven't sent any tracked reactions yet in this server.")
        return

    embed = discord.Embed(
        title=f"✨ {ctx.author.display_name}'s Top 10 Most Used Reactions ✨",
        description="Here are the reactions you've sent the most:",
        color=discord.Color.blue()
    )

    for i, (emoji_name, emoji_count) in enumerate(results):
        embed.add_field(
            name=f"#{i+1} {emoji_name}",
            value=f"Count: {emoji_count}",
            inline=False
        )

    await ctx.followup.send(embed=embed)



# --- Updated: Added guild_id to reaction_events table creation for filtering ---
# You'll need to manually add the 'guild_id' column to your existing 'reaction_events'
# table if you've already run the bot, or delete the 'reactions.db' file to regenerate.
#
# To add the column to an existing database:
# ALTER TABLE reaction_events ADD COLUMN guild_id INTEGER;
# UPDATE reaction_events SET guild_id = (SELECT guild_id FROM messages WHERE messages.message_id = reaction_events.message_id);
#
# The setup_database() function at the top has been updated to include it for new databases.

# --- Placeholder for Other Commands ---
# (Same as before, implement these using similar database query patterns)

# --- Run the Bot ---

if __name__ == "__main__":
    if DISCORD_BOT_TOKEN is None:
        print("ERROR: DISCORD_BOT_TOKEN environment variable not set.")
        print("Please set the 'DISCORD_BOT_TOKEN' environment variable with your actual bot token.")
        print("Example (Linux/macOS): export DISCORD_BOT_TOKEN='YOUR_NEW_TOKEN_HERE'")
        print("Example (Windows Cmd): set DISCORD_BOT_TOKEN=YOUR_NEW_TOKEN_HERE")
        print("Example (Windows PowerShell): $env:DISCORD_BOT_TOKEN='YOUR_NEW_TOKEN_HERE'")
        print("You can get a new token from the Discord Developer Portal after resetting the old one.")
    
        




@bot.slash_command(name="topemojiusers", description="Shows the top 10 users by reactions received for a specific emoji.")
async def top_emoji_users(ctx: discord.ApplicationContext, emoji: str):
    """
    Shows the top 10 users by reactions received for a specific emoji.
    """
    await ctx.defer()
    if not ctx.guild:
        await ctx.followup.send("This command can only be used in a server.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT
            u.username,
            u.discriminator,
            SUM(CASE WHEN re.event_type = 'add' THEN 1 ELSE -1 END) AS emoji_reactions
        FROM
            reaction_events re
        JOIN
            users u ON re.message_author_id = u.user_id
        WHERE
            re.guild_id = ? AND re.emoji_name = ?
        GROUP BY
            u.user_id
        ORDER BY
            emoji_reactions DESC
        LIMIT 10
    ''', (ctx.guild.id, emoji))
    results = cursor.fetchall()
    conn.close()

    if not results:
        await ctx.followup.send(f"No reaction data for emoji {emoji} yet.")
        return

    embed = discord.Embed(
        title=f"🏆 Top 10 Users by '{emoji}' Reactions Received 🏆",
        color=discord.Color.orange()
    )
    for i, (username, discriminator, emoji_reactions) in enumerate(results):
        display_name = f"{username}#{discriminator}" if discriminator != "0" else username
        embed.add_field(
            name=f"#{i+1} {display_name}",
            value=f"Total '{emoji}' Reactions: {emoji_reactions}",
            inline=False
        )
    await ctx.followup.send(embed=embed)

@bot.slash_command(name="servertopreactions", description="Shows the server's top 10 most used reactions.")
async def server_top_reactions(ctx: discord.ApplicationContext):
    """
    Shows the server's top 10 most used reactions.
    """
    await ctx.defer()
    if not ctx.guild:
        await ctx.followup.send("This command can only be used in a server.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT
            emoji_name,
            SUM(CASE WHEN event_type = 'add' THEN 1 ELSE -1 END) AS emoji_count
        FROM
            reaction_events
        WHERE
            guild_id = ?
        GROUP BY
            emoji_name
        ORDER BY
            emoji_count DESC
        LIMIT 10
    ''', (ctx.guild.id,))
    results = cursor.fetchall()
    conn.close()

    if not results:
        await ctx.followup.send("No reaction data available yet for this server.")
        return

    embed = discord.Embed(
        title="🔥 Server's Top 10 Most Used Reactions 🔥",
        color=discord.Color.red()
    )
    for i, (emoji_name, emoji_count) in enumerate(results):
        embed.add_field(
            name=f"#{i+1} {emoji_name}",
            value=f"Count: {emoji_count}",
            inline=False
        )
    await ctx.followup.send(embed=embed)

@bot.slash_command(name="usertopreceived", description="Shows a user's top 10 most received reactions.")
async def user_top_received(ctx: discord.ApplicationContext, user: discord.User = None):
    """
    Shows the top 10 most received reactions for a user.
    """
    await ctx.defer()
    if not ctx.guild:
        await ctx.followup.send("This command can only be used in a server.")
        return

    target_user = user or ctx.author

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT
            emoji_name,
            SUM(CASE WHEN event_type = 'add' THEN 1 ELSE -1 END) AS emoji_count
        FROM
            reaction_events
        WHERE
            message_author_id = ? AND guild_id = ?
        GROUP BY
            emoji_name
        ORDER BY
            emoji_count DESC
        LIMIT 10
    ''', (target_user.id, ctx.guild.id))
    results = cursor.fetchall()
    conn.close()

    if not results:
        await ctx.followup.send(f"No reactions received yet for {target_user.display_name}.")
        return

    embed = discord.Embed(
        title=f"🎉 {target_user.display_name}'s Top 10 Most Received Reactions 🎉",
        color=discord.Color.green()
    )
    for i, (emoji_name, emoji_count) in enumerate(results):
        embed.add_field(
            name=f"#{i+1} {emoji_name}",
            value=f"Count: {emoji_count}",
            inline=False
        )
    await ctx.followup.send(embed=embed)

@bot.slash_command(name="topmessages", description="Shows the top 10 messages with the most reactions (optionally for a specific emoji).")
async def top_messages(ctx: discord.ApplicationContext, emoji: str = None):
    """
    Shows the top 10 messages with the most reactions, optionally filtered by emoji.
    """
    await ctx.defer()
    if not ctx.guild:
        await ctx.followup.send("This command can only be used in a server.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    if emoji:
        cursor.execute('''
            SELECT
                m.message_id,
                u.username,
                u.discriminator,
                SUM(CASE WHEN re.event_type = 'add' THEN 1 ELSE -1 END) AS reaction_count
            FROM
                reaction_events re
            JOIN
                messages m ON re.message_id = m.message_id
            JOIN
                users u ON m.author_id = u.user_id
            WHERE
                re.guild_id = ? AND re.emoji_name = ?
            GROUP BY
                m.message_id
            ORDER BY
                reaction_count DESC
            LIMIT 10
        ''', (ctx.guild.id, emoji))
    else:
        cursor.execute('''
            SELECT
                m.message_id,
                u.username,
                u.discriminator,
                SUM(CASE WHEN re.event_type = 'add' THEN 1 ELSE -1 END) AS reaction_count
            FROM
                reaction_events re
            JOIN
                messages m ON re.message_id = m.message_id
            JOIN
                users u ON m.author_id = u.user_id
            WHERE
                re.guild_id = ?
            GROUP BY
                m.message_id
            ORDER BY
                reaction_count DESC
            LIMIT 10
        ''', (ctx.guild.id,))
    results = cursor.fetchall()
    conn.close()

    if not results:
        await ctx.followup.send("No reaction data available yet for messages in this server.")
        return

    title = f"💬 Top 10 Messages by {'`' + emoji + '` ' if emoji else ''}Reactions"
    embed = discord.Embed(
        title=title,
        color=discord.Color.purple()
    )
    for i, (message_id, username, discriminator, reaction_count) in enumerate(results):
        display_name = f"{username}#{discriminator}" if discriminator != "0" else username
        embed.add_field(
            name=f"#{i+1} by {display_name}",
            value=f"Message ID: `{message_id}`\nReactions: {reaction_count}",
            inline=False
        )
    await ctx.followup.send(embed=embed)

if __name__ == "__main__":
    import asyncio
    nest_asyncio.apply()
    asyncio.run(bot.start(DISCORD_BOT_TOKEN))


