from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from sqlalchemy import text, false
from dbSetup import engine, update
from faker import Faker

app = Flask(__name__)
fake = Faker()
#test
@app.route('/rides/request', methods=['POST'])
def request_ride():
    flag=False
    data = request.json
    rider_name = data.get("rider_name")
    drop_off = data.get("drop_off")
    pick_up = data.get("pick_up")






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

        result = conn.execute(
            text("""
                      SELECT ST_DistanceSphere(
                   ST_GeomFromEWKB(decode(:pickup, 'hex')),
                   ST_GeomFromEWKB(decode(:dropoff, 'hex'))
                                    ) / 1000 AS distance_km

                   """),
            {"pickup": pick_up, "dropoff": drop_off}
        )
        distance_km = result.scalar()


        speed_kmh = 40
        #duration_hours = distance_km / speed_kmh
        #estimated_duration_minutes = int(duration_hours * 60)

        estimated_price = round(distance_km * 2, 2)

        #estimated_arrival_time = datetime.utcnow() + timedelta(minutes=estimated_duration_minutes)


        conn.execute(
            text("""
                       INSERT INTO ride (ride_id,driver_id,rider_id, status,total_price,duration,distance_traveled, pickup_location_geo, dropoff_location_geo)
                       VALUES (:ride_id, :driver_id, :rider_id, :status, :total_price,
            :duration, :distance_traveled, :pickup_location_geo, :dropoff_location_geo)
                   """),
            {
                "ride_id": fake.random_number(7,fix_len=True),
                "driver_id": 0000000,
                "rider_id": rider_id,
                "status": "requested",
                "total_price": estimated_price,
                "duration": fake.time(),
                "distance_traveled": distance_km,
                "pickup_location_geo": pick_up,
                "dropoff_location_geo": drop_off
            }
        )


        conn.commit()


    return jsonify({
        "message": "Ride requested successfully  ",
        "For rider": rider_name,
    }), 201

@app.route('/rides/accept', methods=['POST'])
def accept_ride():
    data = request.json
    ride_id=data.get("ride_id")
    driver_id=data.get("driver_id")


    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT status FROM driver WHERE driver_id = :driver_id"),
            {"driver_id": driver_id}

        )
        driver=result.fetchone()
        status = result.scalar()
        if not driver:
            return jsonify({"error": "Driver not found"}), 404
        elif status =="offline":
            return jsonify({"error": "Driver offline"}), 404

        conn.execute(
            text("UPDATE ride SET status = :status WHERE ride_id = :ride_id AND driver_id=:driver_id"),
            {"ride_id": ride_id, "driver_id": driver_id, "status": 'accepted'}
        )

        conn.commit()



    return jsonify({
            "message": "Ride accepted successfully  ",
            "for driver": driver_id,
        }), 201


if __name__ == '__main__':
    app.run(debug=True)