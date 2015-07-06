import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg_types

metadata = sa.MetaData()

log_table = sa.Table('logs_ctrip', metadata,
                     sa.Column('tstamp', sa.DateTime,           quote=False, default=sa.func.now()),
                     sa.Column('function', sa.String(100),      quote=False),
                     sa.Column('level', sa.SmallInteger,        quote=False),
                     sa.Column('short_message', sa.String(255), quote=False),
                     sa.Column('event_info', pg_types.JSON(),   quote=False), 
                     schema='chinese_otas', quote=False)

staging_table = sa.Table('ctrip_hotels_staging', metadata,
                         sa.Column('hotelId',           sa.Integer,      quote=False),
                         sa.Column('hotelName',         sa.String(255),  quote=False),
                         sa.Column('averageRate',       sa.String(100),  quote=False),
                         sa.Column('hotelFacility',     sa.String(10),   quote=False),
                         sa.Column('hotelAmount',       sa.Numeric(14, 2, True), quote=False),
                         sa.Column('hotelRating',       sa.Float,        quote=False),
                         sa.Column('hotelReviewsCount', sa.Integer,      quote=False),
                         sa.Column('hotelStarCount',    sa.Float,        quote=False),
                         sa.Column('hotelStarLicence',  sa.Float,        quote=False),
                         sa.Column('lat',               sa.Float,        quote=False),
                         sa.Column('lng',               sa.Float,        quote=False),
                         sa.Column('isBookable',        sa.Boolean,      quote=False),
                         sa.Column('hasCoupon',         sa.Boolean,      quote=False),
                         sa.Column('hasPromotion',      sa.Boolean,      quote=False),
                         sa.Column('hasPromotionCode',  sa.Boolean,      quote=False),
                         sa.Column('currency',          sa.String(3),    quote=False),
                         sa.Column('couponAmount',      sa.String(255),  quote=False),
                         sa.Column('couponTxt',         sa.String(255),  quote=False),
                         sa.Column('couponNumeric',     sa.Numeric(14, 2, True), quote=False),
                         sa.Column('city_id',           sa.Integer), 
                         schema='chinese_otas', quote=False)
