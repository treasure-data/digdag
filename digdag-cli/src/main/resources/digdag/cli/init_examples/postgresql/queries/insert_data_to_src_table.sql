DELETE FROM example_access_logs;

INSERT INTO
  example_access_logs(time, path, code, agent, method)
VALUES 
  (1412380793, '/item/sports/4642', 200, 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:9.0.1) Gecko/20100101 Firefox/9.0.1', 'GET'),
  (1412380784, '/category/finance', 400, 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; YTB730; GTB7.2; EasyBits GO v1.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C)',  'GET'),
  (1412380775, '/item/electronics/954', 200, 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:9.0.1) Gecko/20100101 Firefox/9.0.1', 'POST'),
  (1412380766, '/category/networking', 200, 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; YTB730; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C)',   'GET'),
  (1412380757, '/category/books', 200, 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; YTB730; GTB7.2; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E; Media Center PC 6.0)',  'GET'),
  (1412380748, '/item/finance/3775', 400, 'Mozilla/5.0 (Windows NT 6.0; rv:10.0.1) Gecko/20100101 Firefox/10.0.1',   'POST'),
  (1412380739, '/item/networking/540', 200, 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.46 Safari/535.11',    'POST'),
  (1412380730, '/item/health/1326', 200, 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:9.0.1) Gecko/20100101 Firefox/9.0.1', 'GET'),
  (1412380721, '/category/finance?from=10', 200, 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',    'GET'),
  (1412380712, '/item/computers/959', 500, 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.77 Safari/535.7',   'GET')

