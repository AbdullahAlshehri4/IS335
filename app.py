from datetime import datetime, timedelta
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
                "drop_off_y": pick_up_y,
                "drop_off_x": pick_up_x,
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
    driver_id=str(data.get("driver_id"))
    with engine.connect() as conn:
        ride_id = conn.execute(
            text("""
 SELECT r.ride_id,
                        ST_DistanceSphere(d.location, r.pickup_location_geo) / 1000             AS distance_km,
                        (ST_DistanceSphere(d.location, r.pickup_location_geo) / 1000) / 40 * 60 AS estimated_minutes
                 FROM driver d
                          CROSS JOIN ride r
                 WHERE d.driver_id= :driver_id 
                   AND d.status = 'online'
                   AND r.status = 'requested'
                   AND r.driver_id IS NULL 
                   AND ST_DistanceSphere(d.location, r.pickup_location_geo) / 1000 <= 5
                   AND NOT EXISTS (
      SELECT 1
      FROM ride r2
      WHERE r2.driver_id = d.driver_id
        AND r2.status IN ('requested', 'accepted','in_progress')
  )

                 ORDER BY distance_km
                 LIMIT 1                 """),
            {"driver_id": driver_id}
        )
        if ride_id:
            ride_id,distance_km,estimated_minutes=ride_id.fetchone()
            print(ride_id,distance_km,estimated_minutes)



            driver_status = conn.execute(
                    text("SELECT status FROM driver WHERE driver_id = :driver_id"),
                    {"driver_id": driver_id}

                )
            driver_status=driver_status.scalar()
            ride_status=conn.execute(text(
                "Select status from ride where ride_id=:ride_id"),
                {"ride_id":ride_id}
            )
            ride_status=ride_status.scalar()
            if driver_status=="online" and ride_status=="requested":
                result2 = conn.execute(
                    text('select total_price from ride where ride_id=:ride_id'),
                    {'ride_id': ride_id}
                )
                total_price=result2.scalar()

                type= conn.execute(text("Select d.type from vehicle d where driver_id=:driver_id"),
                                      {"driver_id": driver_id})
                type=type.scalar()
                base_price=total_price
                if type=="premium":
                    total_price*=1.1
                elif type=="family":
                    total_price*=1.25

                surge = conn.execute(text( #surge
                    "SELECT s.multiplier "
                    "FROM ride r "
                    "JOIN surge_areas s ON ST_DistanceSphere(r.pickup_location_geo, s.location) < 5000 "
                    "WHERE r.ride_id = :ride_id "
                    "ORDER BY ST_DistanceSphere(r.pickup_location_geo, s.location) "
                    "LIMIT 1"
                ), {"ride_id": ride_id})
                if surge.fetchone():
                    surge = surge.fetchone()
                    total_price+=(surge[0]-1)*base_price

                conn.execute(
                    text("UPDATE ride SET status = :status,total_price= :total_price,driver_id=:driver_id WHERE ride_id = :ride_id"),
                    {"ride_id": ride_id, "driver_id": driver_id, "status": 'accepted',"total_price":total_price}
                )

                conn.commit()
                return jsonify({
                    "message": "Ride accepted successfully  ",
                    "for driver": driver_id,
                    "test": total_price,
                }), 201
            return jsonify({
                "message": "Driver is not online" }) if driver_status=="online" else jsonify({
                "message": "Ride is already accepted"
            })
        else:
            return jsonify({
                "message": "No near rides "
            })


if __name__ == '__main__':
    app.run(debug=True)