from flask import Flask, request, jsonify
from sqlalchemy import text, false
from dbSetup import engine
from faker import Faker

app = Flask(__name__)
fake = Faker()
#test
@app.route('/rides/request', methods=['POST'])
def request_ride():
    data = request.json
    rider_name = data.get("rider_name")
    drop_off_x = data.get("drop_off_x")
    drop_off_y = data.get("drop_off_y")
    pick_up_x = data.get("pick_up_x")
    pick_up_y = data.get("pick_up_y")






    if not rider_name:
        return jsonify({"error": "Missing rider_name"}), 400


    with engine.connect() as conn:

        result = conn.execute(
        text("SELECT rider_id, email, phone_number FROM rider WHERE name like :name LIMIT 1"),
            {"name": rider_name}
        )
        rider = result.fetchone()
        if not rider:
            return jsonify({"error": "Rider not found"}), 404

        rider_id, email, phone_number = rider

        result = conn.execute(
            text("""
                      SELECT ST_DistanceSphere(
                   ST_MakePoint(:drop_off_y,:drop_off_x),
                   ST_MakePoint(:pickup_y,:pickup_x)
                                    ) / 1000 AS distance_km

                   """),
            {"pickup_y": pick_up_y,"pickup_x": pick_up_x,"drop_off_y": drop_off_y,"drop_off_x": drop_off_x}
        )
        distance_km = result.scalar()



        speed_kmh = 40
        duration_hours = distance_km / speed_kmh
        estimated_duration_minutes = int(duration_hours * 60)

        estimated_price = round(distance_km * 5, 2)


        conn.execute(
            text("""
                       INSERT INTO ride (ride_id,rider_id, status,total_price,duration,distance_traveled, pickup_location_geo, dropoff_location_geo)
                       VALUES (:ride_id, :rider_id, :status, :total_price,
            :duration, :distance_traveled, ST_MakePoint(:drop_off_y,:drop_off_x), ST_MakePoint(:pickup_off_y,:pickup_off_x))
                   """),
            {
                "ride_id": fake.random_number(7,fix_len=True),
                "rider_id": rider_id,
                "status": "requested",
                "total_price": estimated_price,
                "duration": estimated_duration_minutes,
                "distance_traveled": distance_km,
                "pickup_off_y": pick_up_y,
                "pickup_off_x": pick_up_x,
                "drop_off_y": drop_off_y,
                "drop_off_x": drop_off_x,
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
    driver_id = str(data.get("driver_id"))
    with engine.connect() as conn:
        ride_id_result = conn.execute(
            text("""
                SELECT r.ride_id,
                       ST_DistanceSphere(d.location, r.pickup_location_geo) / 1000 AS distance_km
                FROM driver d
                         CROSS JOIN ride r
                WHERE d.driver_id = :driver_id
                  AND d.status = 'online'
                  AND r.status = 'requested'
                  AND r.driver_id IS NULL
                  AND ST_DistanceSphere(d.location, r.pickup_location_geo) / 1000 <= 5
                  AND NOT EXISTS (
                      SELECT 1
                      FROM ride r2
                      WHERE r2.driver_id = d.driver_id
                        AND r2.status IN ('requested', 'accepted', 'in_progress')
                  )
                ORDER BY distance_km
                LIMIT 1
            """),
            {"driver_id": driver_id}
        )
        row = ride_id_result.fetchone()
        if row:
            ride_id, _ = row
        else:
            return jsonify({"message": "Driver is not online"})

        driver_status_result = conn.execute(
            text("SELECT status FROM driver WHERE driver_id = :driver_id"),
            {"driver_id": driver_id}
        )
        driver_status = driver_status_result.scalar()

        ride_status_result = conn.execute(
            text("SELECT status FROM ride WHERE ride_id = :ride_id"),
            {"ride_id": ride_id}
        )
        ride_status = ride_status_result.scalar()

        if driver_status == "online" and ride_status == "requested":
            total_price_result = conn.execute(
                text("SELECT total_price FROM ride WHERE ride_id = :ride_id"),
                {'ride_id': ride_id}
            )
            total_price = total_price_result.scalar()

            vehicle_type_result = conn.execute(
                text("SELECT d.type FROM vehicle d WHERE driver_id = :driver_id"),
                {"driver_id": driver_id}
            )
            vehicle_type = vehicle_type_result.scalar()
            base_price = total_price

            if vehicle_type == "premium":
                total_price *= 1.1
            elif vehicle_type == "family":
                total_price *= 1.25

            surge_result = conn.execute(
                text("""
                    SELECT s.multiplier
                    FROM ride r
                             JOIN surge_areas s ON ST_DistanceSphere(r.pickup_location_geo, s.location) < 5000
                    WHERE r.ride_id = :ride_id
                    ORDER BY ST_DistanceSphere(r.pickup_location_geo, s.location)
                    LIMIT 1
                """),
                {"ride_id": ride_id}
            )
            surge_row = surge_result.fetchone()
            if surge_row:
                total_price += (surge_row[0] - 1) * base_price

            conn.execute(
                text("""
                    UPDATE ride
                    SET status = :status,
                        total_price = :total_price,
                        driver_id = :driver_id
                    WHERE ride_id = :ride_id
                """),
                {
                    "ride_id": ride_id,
                    "driver_id": driver_id,
                    "status": 'accepted',
                    "total_price": total_price
                }
            )

            conn.commit()
            return jsonify({
                "message": "Ride accepted successfully",
                "for_driver": driver_id,
                "test": total_price,
            }), 201

        if driver_status != "online":
            return jsonify({"message": "Driver is not online"})
        else:
            return jsonify({"message": "Ride is already accepted"})


if __name__ == '__main__':
    app.run(debug=True)