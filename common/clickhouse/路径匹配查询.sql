select deviceId,
       sequenceMatch('.*(?1).*(?2).*(?3)')(
                     toDateTime(`timeStamp`),
                     eventId = 'Z' and properties['p3'] = 'v8',
                     eventId = 'I' and properties['p2'] = 'v6',
                     eventId = 'A' and properties['p8'] = 'v7'
           ) as is_match3,
       sequenceMatch('.*(?1).*(?2).*')(
                     toDateTime(`timeStamp`),
                     eventId = 'Z' and properties['p3'] = 'v8',
                     eventId = 'I' and properties['p2'] = 'v6',
                     eventId = 'A' and properties['p8'] = 'v7'
           ) as is_match2,
       sequenceMatch('.*(?1).*')(
                     toDateTime(`timeStamp`),
                     eventId = 'Z' and properties['p3'] = 'v8',
                     eventId = 'I' and properties['p2'] = 'v6',
                     eventId = 'A' and properties['p8'] = 'v7'
           ) as is_match1
from default.event_detail
where deviceId = '008788'
  and timeStamp >= 0
  and timeStamp < 5928492919183
  and (
        (eventId = 'Z' and properties['p3'] = 'v8')
        or (eventId = 'I' and properties['p2'] = 'v6')
        or (eventId = 'A' and properties['p8'] = 'v7')
    )
group by deviceId;