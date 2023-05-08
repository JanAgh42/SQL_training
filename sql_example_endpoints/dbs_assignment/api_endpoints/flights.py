from ..database import database as db
from fastapi import APIRouter

flights_router = APIRouter()

@flights_router.get("/v1/passengers/{passenger_id}/companions")
async def companions(passenger_id):
    
    passengers = db.single_record(
        f"""SELECT array_to_json(array_agg(passengers))
        from (
            SELECT
                t.passenger_id AS id,
                t.passenger_name AS name,
                COUNT(t.passenger_id) AS flights_count,
                ARRAY_AGG(tf.flight_id ORDER BY tf.flight_id ASC) AS flights
            from bookings.tickets AS t
            INNER JOIN bookings.ticket_flights AS tf
            ON t.ticket_no = tf.ticket_no
            WHERE t.passenger_id != '{ passenger_id }' AND tf.flight_id IN (
                SELECT inner_tf.flight_id
                from bookings.tickets AS inner_t
                INNER JOIN bookings.ticket_flights AS inner_tf
                ON inner_t.ticket_no = inner_tf.ticket_no
                WHERE inner_t.passenger_id = '{ passenger_id }'
            )
            GROUP BY t.passenger_id, t.passenger_name
            ORDER BY flights_count DESC, id ASC
        ) passengers;"""
    )

    db.close_cursor()

    return {
        'results': passengers[0]
    }

@flights_router.get("/v1/bookings/{booking_id}")
async def bookings(booking_id):

    bookings_dict: dict = {
        "book_date": "",
        "boarding_passes": []
    }
    
    bookings = db.all_records(
        f"""SELECT DISTINCT
	        b.book_ref AS id,
	        b.book_date, 
	        t.ticket_no AS bp_id,
	        t.passenger_id,
	        t.passenger_name,
            bp.boarding_no,
            f.flight_no,
            bp.seat_no AS seat,
            f.aircraft_code,
            f.arrival_airport,
            f.departure_airport,
            f.scheduled_arrival,
            f.scheduled_departure
        from bookings.bookings AS b
        INNER JOIN bookings.tickets AS t
        ON b.book_ref = t.book_ref
        INNER JOIN bookings.boarding_passes AS bp
        ON t.ticket_no = bp.ticket_no
        INNER JOIN bookings.flights AS f
        ON bp.flight_id = f.flight_id
        WHERE b.book_ref = '{ booking_id }'
        ORDER BY bp_id ASC, bp.boarding_no ASC;"""
    )

    db.close_cursor()

    if bookings.__len__() <= 0:
        return {
            'result': 'Booking id does not exist'
        }

    bookings_dict["id"] = bookings[0][0]
    bookings_dict["book_date"] = bookings[0][1]

    for booking in bookings:
        bookings_dict["boarding_passes"].append(
            {
                "id": booking[2],
                "passenger_id": booking[3],
                "passenger_name": booking[4],
                "boarding_no": booking[5],
                "flight_no": booking[6],
                "seat": booking[7],
                "aircraft_code": booking[8],
                "arrival_airport": booking[9],
                "departure_airport": booking[10],
                "scheduled_arrival": booking[11],
                "scheduled_departure": booking[12]
            }
        )

    return {
        'result': bookings_dict
    }

@flights_router.get("/v1/flights/late-departure/{delay}")
async def late_departure(delay):

    late_departures = db.single_record(
        f"""SELECT array_to_json(array_agg(late))
        from (   
            SELECT DISTINCT
                f.flight_id,
                f.flight_no,
                FLOOR(
                    EXTRACT(
                        epoch from (f.actual_departure - f.scheduled_departure)
                    ) / 60
                ) AS delay
            from bookings.flights AS f
            WHERE CAST(
                EXTRACT(epoch from (f.actual_departure - f.scheduled_departure)) AS INT
            ) / 60 >= { delay }
            ORDER BY delay DESC, f.flight_id ASC
        ) late;"""
    )
    
    db.close_cursor()

    return {
        'results': late_departures[0]
    }

@flights_router.get("/v1/top-airlines")
async def top_airlines(limit = 5):
    
    top_airlines = db.single_record(
        f"""SELECT array_to_json(array_agg(top))
        from (
            SELECT
                f.flight_no,
                COUNT(bp.flight_id) AS count
            from bookings.flights AS f
            INNER JOIN bookings.boarding_passes AS bp
            ON f.flight_id = bp.flight_id
            WHERE f.status = 'Arrived'
            GROUP BY f.flight_no
            ORDER BY count DESC, f.flight_no ASC
            LIMIT { limit }
        ) top;"""
    )

    db.close_cursor()

    return {
        'results': top_airlines[0]
    }

@flights_router.get("/v1/departures")
async def departures(airport = 'KJA', day = 7):
    
    departures = db.single_record(
        f"""SELECT array_to_json(array_agg(departures))
        from (
            SELECT
                f.flight_id,
                f.flight_no,
                f.scheduled_departure
            from bookings.flights AS f
            WHERE
                f.status = 'Scheduled' AND
                f.departure_airport = '{ airport }' AND
                EXTRACT(isodow from f.scheduled_departure) = { day }
            ORDER BY f.scheduled_departure ASC, f.flight_id ASC
        ) departures;"""
    )

    db.close_cursor()

    return {
        'results': departures[0]
    }

@flights_router.get("/v1/airports/{airport}/destinations")
async def destinations(airport):

    airports_list: list = []

    airports = db.all_records(
        f"""SELECT DISTINCT 
            f.arrival_airport
        from bookings.flights AS f
        WHERE f.departure_airport = '{ airport }'
        ORDER BY f.arrival_airport ASC;"""
    )

    db.close_cursor()

    if airports.__len__() <= 0:
        return {
            'result': 'Airport does not exist'
        }

    for aport in airports:
        airports_list.append(aport[0])

    return {
        'results': airports_list
    }

@flights_router.get("/v1/airlines/{flight_no}/load")
async def airlines(flight_no):

    airlines = db.single_record(
        f"""SELECT array_to_json(array_agg(airlines))
        from (
            WITH aircraft_detail(code, seats) AS (
				SELECT
					s.aircraft_code,
					COUNT(s.seat_no)
				from bookings.seats AS s
				GROUP BY s.aircraft_code
			)
            SELECT
                f.flight_id AS id,
                COUNT(DISTINCT tf.ticket_no) AS load,
				(
					SELECT ad.seats
					FROM aircraft_detail AS ad
					WHERE ad.code = f.aircraft_code
				) AS aircraft_capacity,
                ROUND(
					CAST(COUNT(DISTINCT tf.ticket_no) AS numeric) /
					CAST((
						SELECT ad.seats
						FROM aircraft_detail AS ad
						WHERE ad.code = f.aircraft_code) AS numeric) * 100,
                    2
                )::REAL AS percentage_load
            from bookings.flights AS f
            LEFT JOIN bookings.ticket_flights AS tf
            ON f.flight_id = tf.flight_id
            WHERE f.flight_no = '{ flight_no }'
            GROUP BY f.flight_id, f.aircraft_code
            ORDER BY id ASC
        ) airlines;"""
    )

    db.close_cursor()

    return {
        'results': airlines[0]
    }

@flights_router.get("/v1/airlines/{flight_no}/load-week")
async def averages(flight_no):

    averages = db.all_records(
        f"""WITH aircraft_detail(code, seats) AS (
            SELECT
                s.aircraft_code,
                COUNT(s.seat_no)
            from bookings.seats AS s
            GROUP BY s.aircraft_code
        ),
        weeks_detail(day, code, avg) AS (
            SELECT
                EXTRACT(isodow from f.scheduled_departure),
                f.aircraft_code,
                CAST(COUNT(tf.ticket_no) AS numeric) / CAST(COUNT(DISTINCT f.flight_id) AS numeric)
            from bookings.flights AS f
            INNER JOIN bookings.ticket_flights AS tf
            ON f.flight_id = tf.flight_id
            WHERE f.flight_no = '{ flight_no }'
            GROUP BY EXTRACT(isodow from f.scheduled_departure), f.aircraft_code
        )
        SELECT
            wd.day,
            ROUND((wd.avg / CAST(ad.seats AS numeric)) * 100, 2)::REAL
        from weeks_detail AS wd
        INNER JOIN aircraft_detail AS ad
        ON wd.code = ad.code;"""
    )

    db.close_cursor()

    if averages.__len__() <= 0:
        return {
            'result': 'Flight number does not exist'
        }
    
    return {
        'result': {
            "flight_no": flight_no,
            "monday": averages[0][1],
            "tuesday": averages[1][1],
            "wednesday": averages[2][1],
            "thursday": averages[3][1],
            "friday": averages[4][1],
            "saturday": averages[5][1],
            "sunday": averages[6][1]
        }
    }