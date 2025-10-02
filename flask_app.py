from flask import Flask, jsonify
from faker import Faker
import random, json, time
from kafka import KafkaProducer

app = Flask(__name__)
fake = Faker()

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "fifa_players"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8")
)

positions = ["GK","RB","LB","CB","CM","CDM","CAM","RW","LW","ST","CF"]
clubs = ["FC Barcelona", "Real Madrid", "Juventus", "Manchester City", "Chelsea", "PSG"]
nations = ["Argentina","Portugal","Brazil","Germany","France","Spain","England"]

def generate_player():
    sofifa_id = random.randint(100000, 300000)
    short_name = fake.first_name() + " " + fake.last_name()
    long_name = fake.name()
    age = random.randint(16, 40)
    dob = fake.date_of_birth(minimum_age=16, maximum_age=40).strftime("%Y-%m-%d")
    height_cm = random.randint(160, 200)
    weight_kg = random.randint(60, 95)
    nationality = random.choice(nations)
    club_name = random.choice(clubs)
    league_name = fake.word().capitalize() + " League"
    overall = random.randint(50, 99)
    potential = random.randint(overall, 99)
    value_eur = random.randint(100000, 100000000)
    wage_eur = random.randint(1000, 500000)
    player_positions = random.sample(positions, k=2)
    preferred_foot = random.choice(["Right", "Left"])
    work_rate = random.choice(["High/High","High/Low","Medium/Low","Low/Low"])

    return {
        "sofifa_id": sofifa_id,
        "player_url": f"https://sofifa.com/player/{sofifa_id}/{short_name.replace(' ', '-').lower()}",
        "short_name": short_name,
        "long_name": long_name,
        "age": age,
        "dob": dob,
        "height_cm": height_cm,
        "weight_kg": weight_kg,
        "nationality": nationality,
        "club_name": club_name,
        "league_name": league_name,
        "overall": overall,
        "potential": potential,
        "value_eur": value_eur,
        "wage_eur": wage_eur,
        "player_positions": ",".join(player_positions),
        "preferred_foot": preferred_foot,
        "work_rate": work_rate
    }

@app.route("/")
def home():
    return {"message": "FIFA Kafka producer running"}

# push one record
@app.route("/produce", methods=["POST"])
def produce_one():
    p = generate_player()
    key = p["club_name"]
    producer.send(TOPIC, key=key, value=p)
    producer.flush()
    return jsonify({"produced": p})

# continuous fire-and-forget (streams N messages)
@app.route("/produce_many/<int:n>", methods=["POST"])
def produce_many(n):
    sent = 0
    for _ in range(n):
        p = generate_player()
        producer.send(TOPIC, key=p["club_name"], value=p)
        sent += 1
    producer.flush()
    return jsonify({"produced": sent})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
