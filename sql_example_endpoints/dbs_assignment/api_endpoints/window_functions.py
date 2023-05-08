from ..database import database as db
from fastapi import APIRouter

window_functions_router = APIRouter()

@window_functions_router.get("/v3/aircrafts/{aircraft_code}/seats/{seat_choice}")
async def popular_seats(aircraft_code, seat_choice):

    result = dict()
    
    seat = db.single_record(
        f"""WITH flights_to_passes AS (
                SELECT 
                    bp.ticket_no,
                    bp.flight_id,
                    bp.seat_no
                from bookings.boarding_passes AS bp
                WHERE bp.flight_id IN (
                    SELECT f.flight_id
                    from bookings.flights AS f
                    WHERE f.aircraft_code = '{ aircraft_code }')
            )
            SELECT 
                ranked_seats.seat_no AS seat,
                COUNT(ranked_seats.seat_no) AS count
            from (
                SELECT 
                    dense_rank() OVER (PARTITION BY fp.flight_id ORDER BY b.book_date) AS dr,
                    fp.seat_no
                from flights_to_passes AS fp
                INNER JOIN bookings.tickets AS t
                ON fp.ticket_no = t.ticket_no
                INNER JOIN bookings.bookings AS b
                ON b.book_ref = t.book_ref
            ) AS ranked_seats
            WHERE dr = { seat_choice }
            GROUP BY ranked_seats.seat_no
            ORDER BY count DESC LIMIT 1;"""
    )

    db.close_cursor()

    result["seat"] = seat[0]
    result["count"] = seat[1]

    return {
        'result': result
    }

@window_functions_router.get("/v3/air-time/{book_ref}")
async def air_time(book_ref):

    results = list()
    entries = dict()
    ticket = None
    
    airtimes = db.all_records(
        f"""WITH tickets AS (
                SELECT 
                    t.ticket_no,
                    t.passenger_name
                from bookings.tickets AS t
                WHERE t.book_ref = '{ book_ref }'
            ), tickets_flights AS (
                SELECT 
                    tf.ticket_no,
                    tf.flight_id
                from bookings.ticket_flights AS tf
                WHERE tf.ticket_no IN (SELECT ticket_no from tickets)
            )
            SELECT
                t.ticket_no,
                t.passenger_name,
                f.departure_airport,
                f.arrival_airport,
                to_char((f.actual_arrival - f.actual_departure), 'FMHH24:MI:SS') AS flight_time,
                to_char((sum(f.actual_arrival - f.actual_departure) OVER (PARTITION BY t.passenger_name ORDER BY f.actual_departure ASC)), 'FMHH24:MI:SS') AS total_time 
            from tickets AS t
            INNER JOIN tickets_flights AS tf 
            ON tf.ticket_no = t.ticket_no
            INNER JOIN bookings.flights AS f
            ON tf.flight_id = f.flight_id
            ORDER BY t.passenger_name ASC, f.actual_departure ASC;"""
    )

    db.close_cursor()

    if airtimes.__len__() <= 0:
        return {
            'result': 'Booking ref does not exist'
        }

    for time in airtimes:
        if ticket is None or ticket != time[0]:
            if ticket is not None:
                results.append(entries)
                entries = dict()

            ticket = time[0]

            entries["ticket_no"] = ticket
            entries["passenger_name"] = time[1]
            entries["flights"] = list()

        entries["flights"].append({
            "departure_airport": time[2],
            "arrival_airport": time[3],
            "flight_time": time[4],
            "total_time": time[5]
        })
    
    results.append(entries)

    return {
        'results': results
    }

@window_functions_router.get("/v3/airlines/{flight_no}/top_seats")
async def top_seats(limit = 5):

    return {
        'results': "Not implemented"
    }

@window_functions_router.get("/v3/aircrafts/{aircraft_code}/top-incomes")
async def top_incomes(aircraft_code):

    results = list()
    
    top_income_months = db.all_records(
        f"""WITH flight_ids AS (
                SELECT 
                    f.flight_id,
                    date_trunc('day', f.actual_departure)::DATE AS date
                from bookings.flights AS f
                WHERE f.actual_departure IS NOT NULL AND f.aircraft_code = '{ aircraft_code }'
            ),
            sums AS (
                SELECT
                    EXTRACT(DAY from fi.date)::TEXT AS day,
                    CONCAT(
                        EXTRACT(YEAR from fi.date), '-', EXTRACT(MONTH from fi.date)
                    ) AS year_month,
                    ROW_NUMBER() OVER (
                        PARTITION BY CONCAT(
                            EXTRACT(YEAR from fi.date), '-', EXTRACT(MONTH from fi.date)
                        ) ORDER BY SUM(tf.amount) DESC) AS row,
                    SUM(tf.amount) AS sum
                from flight_ids AS fi
                INNER JOIN bookings.ticket_flights AS tf
                ON fi.flight_id = tf.flight_id
                GROUP BY fi.date
            )
            SELECT
                TRUNC(s.sum) AS total_amount,
                s.year_month AS month,
                s.day
            from sums AS s
            WHERE s.row = 1
            ORDER BY sum DESC, s.year_month ASC;"""
    )

    db.close_cursor()

    if top_income_months.__len__() <= 0:
        return {
            'result': 'Aircraft code does not exist'
        }
    
    for income_month in top_income_months:
        entry = dict()

        entry["total_amount"] = income_month[0]
        entry["month"] = income_month[1]
        entry["day"] = income_month[2]

        results.append(entry)

    return {
        'results': results
    }