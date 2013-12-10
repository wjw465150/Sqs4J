package org.sqs4j;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class HttpRequestHandler extends SimpleChannelInboundHandler<Object> {
	private final Sqs4jApp _app;

	public HttpRequestHandler(Sqs4jApp app) {
		_app = app;
	}

	private boolean checkUser(FullHttpRequest request, FullHttpResponse response, Charset charsetObj) throws IOException {
		String username = "";
		String password = "";
		String userPass = request.headers().get(HttpHeaders.Names.AUTHORIZATION);

		if (null != userPass) {
			userPass = userPass.substring(6, userPass.length());

			userPass = _app.getBASE64DecodeOfStr(userPass, charsetObj.name());
			final int pos = userPass.indexOf(':');
			if (pos > 0) {
				username = userPass.substring(0, pos);
				password = userPass.substring(pos + 1, userPass.length());
			}
		} else {
			response.headers().set(HttpHeaders.Names.WWW_AUTHENTICATE, "Basic realm=\"Sqs4J\"");
			response.setStatus(HttpResponseStatus.UNAUTHORIZED);
			response.content().writeBytes("HTTPSQS_ERROR:需要用户名/口令!".getBytes(charsetObj));
			return false;
		}

		if (_app._conf.adminUser.equals(username) && _app._conf.adminPass.equals(password)) {
			return true;
		} else {
			response.headers().set(HttpHeaders.Names.WWW_AUTHENTICATE, "Basic realm=\"Sqs4J\"");
			response.setStatus(HttpResponseStatus.UNAUTHORIZED);
			response.content().writeBytes("HTTPSQS_ERROR:用户户名/口令 错误!".getBytes(charsetObj));
			return false;
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		//cause.printStackTrace();
		ctx.close();
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
		FullHttpRequest request = (FullHttpRequest) msg;

		//分析URL参数
		QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri(), _app._conf.charsetDefaultCharset);
		Map<String, List<String>> parameters = queryStringDecoder.parameters();

		String charset = (null != parameters.get("charset")) ? parameters.get("charset").get(0) : null; //先从query里查找charset
		Charset charsetObj = _app._conf.charsetDefaultCharset;
		if (null == charset) {
			if (null != request.headers().get("Content-Type")) {
				charset = _app.getCharsetFromContentType(request.headers().get("Content-Type"));
				if (null == charset) {
					charset = _app._conf.defaultCharset;
				} else if (!charset.equalsIgnoreCase(_app._conf.defaultCharset)) { //说明查询参数里指定了字符集,并且与缺省字符集不一致
					charsetObj = Charset.forName(charset);
					queryStringDecoder = new QueryStringDecoder(request.getUri(), charsetObj);
					parameters = queryStringDecoder.parameters();
				}
			} else {
				charset = _app._conf.defaultCharset;
			}
		} else if (!charset.equalsIgnoreCase(_app._conf.defaultCharset)) { //说明查询参数里指定了字符集,并且与缺省字符集不一致
			charsetObj = Charset.forName(charset);
			queryStringDecoder = new QueryStringDecoder(request.getUri(), charsetObj);
			parameters = queryStringDecoder.parameters();
		}

		writeResponse(ctx, request, parameters, charsetObj);

	}

	private void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, Map<String, List<String>> parameters,
	    Charset charsetObj) {
		//接收GET表单参数
		final String httpsqs_input_auth = (null != parameters.get("auth")) ? parameters.get("auth").get(0) : null; // get,put,view的验证密码 
		final String httpsqs_input_name = (null != parameters.get("name")) ? parameters.get("name").get(0) : null; // 队列名称 
		final String httpsqs_input_opt = (null != parameters.get("opt")) ? parameters.get("opt").get(0) : null; //操作类别
		final String httpsqs_input_data = (null != parameters.get("data")) ? parameters.get("data").get(0) : null; //队列数据
		final String httpsqs_input_pos_tmp = (null != parameters.get("pos")) ? parameters.get("pos").get(0) : null; //队列位置点
		final String httpsqs_input_num_tmp = (null != parameters.get("num")) ? parameters.get("num").get(0) : null; //队列总长度
		long httpsqs_input_pos = 0;
		long httpsqs_input_num = 0;
		if (null != httpsqs_input_pos_tmp) {
			httpsqs_input_pos = Long.parseLong(httpsqs_input_pos_tmp);
		}
		if (null != httpsqs_input_num_tmp) {
			httpsqs_input_num = Long.parseLong(httpsqs_input_num_tmp);
		}

		//返回给用户的Header头信息
		FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
		    HttpResponseStatus.OK,
		    Unpooled.buffer(64));
		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain;charset=" + charsetObj.name());
		response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
		response.headers().set(HttpHeaders.Names.CACHE_CONTROL, HttpHeaders.Values.NO_CACHE);

		ByteBuf respBuf = response.content(); //Buffer that stores the response content
		Sqs4jApp._lock.lock();
		try {
			/* 参数是否存在判断 */
			if (null != httpsqs_input_name && null != httpsqs_input_opt && httpsqs_input_name.length() <= 256) {
				/* 入队列 */
				if (httpsqs_input_opt.equals("put")) {
					if (null != _app._conf.auth && !_app._conf.auth.equals(httpsqs_input_auth)) {
						respBuf.writeBytes("HTTPSQS_AUTH_FAILED".getBytes(charsetObj));
					} else {
						/* 优先接收POST正文信息 */
						if (request.getMethod().name().equalsIgnoreCase("POST")) {
							final long now_putpos = _app.httpsqs_now_putpos(httpsqs_input_name);
							if (now_putpos > 0) {
								final String key = httpsqs_input_name + ":" + now_putpos;
								final String value = URLDecoder.decode(request.content().toString(charsetObj), charsetObj.name());
								_app._db.put(key.getBytes(Sqs4jApp.DB_CHARSET), value.getBytes(Sqs4jApp.DB_CHARSET));
								response.headers().set("Pos", now_putpos);
								respBuf.writeBytes("HTTPSQS_PUT_OK".getBytes(charsetObj));
							} else {
								respBuf.writeBytes("HTTPSQS_PUT_END".getBytes(charsetObj));
							}
						} else if (null != httpsqs_input_data) { //如果POST正文无内容，则取URL中data参数的值
							final long now_putpos = _app.httpsqs_now_putpos(httpsqs_input_name);
							if (now_putpos > 0) {
								final String key = httpsqs_input_name + ":" + now_putpos;
								final String value = httpsqs_input_data;
								_app._db.put(key.getBytes(Sqs4jApp.DB_CHARSET), value.getBytes(Sqs4jApp.DB_CHARSET));
								response.headers().set("Pos", now_putpos);
								respBuf.writeBytes("HTTPSQS_PUT_OK".getBytes(charsetObj));
							} else {
								respBuf.writeBytes("HTTPSQS_PUT_END".getBytes(charsetObj));
							}
						} else {
							respBuf.writeBytes("HTTPSQS_PUT_ERROR".getBytes(charsetObj));
						}
					}
				} else if (httpsqs_input_opt.equals("get")) { //出队列
					if (null != _app._conf.auth && !_app._conf.auth.equals(httpsqs_input_auth)) {
						respBuf.writeBytes("HTTPSQS_AUTH_FAILED".getBytes(charsetObj));
					} else {
						final long now_getpos = _app.httpsqs_now_getpos(httpsqs_input_name);
						if (0 == now_getpos) {
							respBuf.writeBytes("HTTPSQS_GET_END".getBytes(charsetObj));
						} else {
							final String key = httpsqs_input_name + ":" + now_getpos;
							final byte[] value = _app._db.get(key.getBytes(Sqs4jApp.DB_CHARSET));
							if (null != value) {
								response.headers().set("Pos", now_getpos);
								byte[] bytesValue = (new String(value, Sqs4jApp.DB_CHARSET)).getBytes(charsetObj);
								respBuf.capacity(bytesValue.length + 16);
								respBuf.writeBytes(bytesValue);
							} else { //@wjw_note: 发生这种状况的可能性极小,那就是设置了"putpos"后,程序突然死掉,没来得及写当前"putpos"指示的位置的数据!
								response.headers().set("Pos", now_getpos);
								respBuf.writeBytes("<null>".getBytes(charsetObj));
							}
						}
					}
					/* 查看单条队列内容 */
				} else if (httpsqs_input_opt.equals("view") && httpsqs_input_pos >= 1 && httpsqs_input_pos <= Sqs4jApp.DEFAULT_MAXQUEUE) {
					if (null != _app._conf.auth && !_app._conf.auth.equals(httpsqs_input_auth)) {
						respBuf.writeBytes("HTTPSQS_AUTH_FAILED".getBytes(charsetObj));
					} else {
						final String httpsqs_output_value = _app.httpsqs_view(httpsqs_input_name, httpsqs_input_pos);
						if (httpsqs_output_value == null) {
							respBuf.writeBytes("HTTPSQS_ERROR_NOFOUND".getBytes(charsetObj));
						} else {
							respBuf.writeBytes(httpsqs_output_value.getBytes(charsetObj));
						}
					}
					/* 查看队列状态（普通浏览方式） */
				} else if (httpsqs_input_opt.equals("status")) {
					String put_times = "1st lap";
					final String get_times = "1st lap";

					final long maxqueue = _app.httpsqs_read_maxqueue(httpsqs_input_name); /* 最大队列数量 */
					final long putpos = _app.httpsqs_read_putpos(httpsqs_input_name); /* 入队列写入位置 */
					final long getpos = _app.httpsqs_read_getpos(httpsqs_input_name); /* 出队列读取位置 */
					long ungetnum = 0;
					if (putpos >= getpos) {
						ungetnum = Math.abs(putpos - getpos); //尚未出队列条数
					} else if (putpos < getpos) {
						ungetnum = Math.abs(maxqueue - getpos + putpos); /* 尚未出队列条数 */
						put_times = "2nd lap";
					}
					respBuf.writeBytes(String.format("HTTP Simple Queue Service (Sqs4J)v%s\n", Sqs4jApp.VERSION)
					    .getBytes(charsetObj));
					respBuf.writeBytes("------------------------------\n".getBytes(charsetObj));
					respBuf.writeBytes(String.format("Queue Name: %s\n", httpsqs_input_name).getBytes(charsetObj));
					respBuf.writeBytes(String.format("Maximum number of queues: %d\n", maxqueue).getBytes(charsetObj));
					respBuf.writeBytes(String.format("Put position of queue (%s): %d\n", put_times, putpos).getBytes(charsetObj));
					respBuf.writeBytes(String.format("Get position of queue (%s): %d\n", get_times, getpos).getBytes(charsetObj));
					respBuf.writeBytes(String.format("Number of unread queue: %d\n", ungetnum).getBytes(charsetObj));
					respBuf.writeBytes(("ScheduleSync running: " + !_app._scheduleSync.isShutdown()).getBytes(charsetObj));
					/* 查看队列状态（JSON方式，方便客服端程序处理） */
				} else if (httpsqs_input_opt.equals("status_json")) {
					String put_times = "1st lap";
					final String get_times = "1st lap";

					final long maxqueue = _app.httpsqs_read_maxqueue(httpsqs_input_name); /* 最大队列数量 */
					final long putpos = _app.httpsqs_read_putpos(httpsqs_input_name); /* 入队列写入位置 */
					final long getpos = _app.httpsqs_read_getpos(httpsqs_input_name); /* 出队列读取位置 */
					long ungetnum = 0;
					if (putpos >= getpos) {
						ungetnum = Math.abs(putpos - getpos); //尚未出队列条数
					} else if (putpos < getpos) {
						ungetnum = Math.abs(maxqueue - getpos + putpos); /* 尚未出队列条数 */
						put_times = "2nd lap";
					}

					respBuf.writeBytes(String.format("{\"name\": \"%s\",\"maxqueue\": %d,\"putpos\": %d,\"putlap\": \"%s\",\"getpos\": %d,\"getlap\": \"%s\",\"unread\": %d, \"sync\": \"%s\"}\n",
					    httpsqs_input_name,
					    maxqueue,
					    putpos,
					    put_times,
					    getpos,
					    get_times,
					    ungetnum,
					    !_app._scheduleSync.isShutdown())
					    .getBytes(charsetObj));
					/* 重置队列 */
				} else if (httpsqs_input_opt.equals("reset")) {
					if (checkUser(request, response, charsetObj)) {
						final boolean reset = _app.httpsqs_reset(httpsqs_input_name);
						if (reset) {
							respBuf.writeBytes(String.format("%s", "HTTPSQS_RESET_OK").getBytes(charsetObj));
						} else {
							respBuf.writeBytes(String.format("%s", "HTTPSQS_RESET_ERROR").getBytes(charsetObj));
						}
					}
					/* 设置最大的队列数量，最小值为10条，最大值为10亿条 */
				} else if (httpsqs_input_opt.equals("maxqueue") && httpsqs_input_num >= 10 && httpsqs_input_num <= Sqs4jApp.DEFAULT_MAXQUEUE) {
					if (checkUser(request, response, charsetObj)) {
						if (_app.httpsqs_maxqueue(httpsqs_input_name, httpsqs_input_num) != 0) {
							respBuf.writeBytes(String.format("%s", "HTTPSQS_MAXQUEUE_OK").getBytes(charsetObj)); //设置成功
						} else {
							respBuf.writeBytes(String.format("%s", "HTTPSQS_MAXQUEUE_CANCEL").getBytes(charsetObj)); //设置取消
						}
					}
					/* 设置定时更新内存内容到磁盘的间隔时间，最小值为1秒，最大值为10亿秒 */
				} else if (httpsqs_input_opt.equals("synctime") && httpsqs_input_num >= 1 && httpsqs_input_num <= Sqs4jApp.DEFAULT_MAXQUEUE) {
					if (checkUser(request, response, charsetObj)) {
						if (_app.httpsqs_synctime((int) httpsqs_input_num) >= 1) {
							respBuf.writeBytes(String.format("%s", "HTTPSQS_SYNCTIME_OK").getBytes(charsetObj));
						} else {
							respBuf.writeBytes(String.format("%s", "HTTPSQS_SYNCTIME_CANCEL").getBytes(charsetObj));
						}
					}
					/* 手动刷新内存内容到磁盘 */
				} else if (httpsqs_input_opt.equals("flush")) {
					if (checkUser(request, response, charsetObj)) {
						_app.flush();
						respBuf.writeBytes(String.format("%s", "HTTPSQS_FLUSH_OK").getBytes(charsetObj));
					}
				} else { /* 命令错误 */
					respBuf.writeBytes(String.format("%s", "HTTPSQS_ERROR:未知的命令!").getBytes(charsetObj));
				}
			} else {
				respBuf.writeBytes(String.format("%s", "HTTPSQS_ERROR:队列名错误!").getBytes(charsetObj));
			}
		} catch (Throwable ex) {
			_app._log.equals(ex);
		} finally {
			Sqs4jApp._lock.unlock();
		}

		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, respBuf.readableBytes());

		// Close the non-keep-alive connection after the write operation is done.
		boolean keepAlive = HttpHeaders.isKeepAlive(request);
		if (!keepAlive) {
			ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
		} else {
			ctx.writeAndFlush(response);
		}
	}
}
