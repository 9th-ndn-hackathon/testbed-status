from flask import Flask
import sqlite3
app = Flask(__name__)
@app.route("/")
def test():
    return "Root"
@app.route("/<src>/<dst>")
def get_status(src, dst):
    connection = sqlite3.connect("status.db")
    conn = connection.cursor()
    conn.execute("SELECT status FROM testbed_status WHERE src=? AND dst=?", (src, dst))
    output = str(conn.fetchone()[0])
    print(output)
    conn.close()
    if output == None:
        return("no")
    else:
        return(output)