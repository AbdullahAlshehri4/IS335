import faker
from flask import Flask, request, jsonify
from sqlalchemy import text, false
from dbSetup import engine, update
from faker import Faker

app = Flask(__name__)
fake = Faker()

@app.route('/rides/request', methods=['POST'])
def request_ride():
    flag=False
    data = request.json
    rider_name = data.get("rider_name")
    drop_off = data.get("drop_off")
    pick_up = data.get("pick_up")
    driver_id = data.get("driver_id")





    if not rider_name:
        return jsonify({"error": "Missing rider_name"}), 400


    with engine.connect() as conn:

        result = conn.execute(
            text("SELECT rider_id, email, phone_number FROM rider WHERE name = :name LIMIT 1"),
            {"name": rider_name}
        )
        rider = result.fetchone()

        if not rider:
            return jsonify({"error": "Rider not found"}), 404

        rider_id, email, phone_number = rider


        conn.execute(
            text("""
                       INSERT INTO ride (ride_id,driver_id,rider_id, status,total_price,duration,distance_traveled, pickup_location_geo, dropoff_location_geo)
                       VALUES (:ride_id, :driver_id, :rider_id, :status, :total_price,
            :duration, :distance_traveled, :pickup_location_geo, :dropoff_location_geo)
                   """),
            {
                "ride_id": fake.random_number(7,fix_len=True),
                "driver_id": driver_id,
                "rider_id": rider_id,
                "status": "requested",
                "total_price": fake.random_int(min=10, max=99),
                "duration": fake.time(),
                "distance_traveled": 0,
                "pickup_location_geo": pick_up,
                "dropoff_location_geo": drop_off
            }
        )


        conn.commit()


    return jsonify({
        "message": "Ride requested successfully  ",
        "rider_id": rider_id,
        "email": email,
        "phone_number": phone_number
    }), 201
if __name__ == '__main__':
    app.run(debug=True)