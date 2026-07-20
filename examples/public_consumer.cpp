#include <sessiongw/sessiongw.hpp>

#include <arrow/record_batch.h>

#include <cstdlib>
#include <iostream>

int main(int argc, char** argv)
{
    if (argc != 6)
    {
        std::cerr << "usage: " << argv[0] << " HOST PORT USER PASSWORD SQL\n";
        return 2;
    }

    sessiongw::WebSocketOptions options;
    options.host = argv[1];
    options.port = static_cast<std::uint16_t>(std::stoul(argv[2]));
    options.user = argv[3];
    options.password = argv[4];

    try
    {
        sessiongw::Session session = sessiongw::Session::connect(options);
        sessiongw::Cursor cursor = session.openPushedQuery(argv[5]);
        std::uint64_t rows = 0;
        for (;;)
        {
            const sessiongw::FetchBatch batch = session.fetch(cursor, 4096);
            if (batch.rows != nullptr) rows += static_cast<std::uint64_t>(batch.rows->num_rows());
            if (batch.end_of_cursor) break;
        }
        session.closeCursor(cursor);
        session.close();
        std::cout << rows << '\n';
    }
    catch (const sessiongw::Error& error)
    {
        std::cerr << sessiongw::toString(error.category()) << ": " << error.what() << '\n';
        return 1;
    }
}
